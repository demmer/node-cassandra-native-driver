#include "async-future.h"
#include <sys/time.h>
#include "metrics.h"

AsyncFuture::AsyncFuture(Metrics* metrics)
{
    metrics_ = metrics;

    uv_mutex_init(&lock_);

    async_ = new uv_async_t();
    uv_async_init(uv_default_loop(), async_, on_async_ready);
    async_->data = this;
}

static void
async_destroy(uv_handle_t* handle)
{
    uv_async_t* async = (uv_async_t*)handle;
    delete async;
}

AsyncFuture::~AsyncFuture()
{
    uv_mutex_destroy(&lock_);

    uv_handle_t* async_handle = (uv_handle_t*)async_;
    uv_close(async_handle, async_destroy);
}

void
AsyncFuture::schedule(callback_t callback, CassFuture* future, void* client, void* data)
{
    Pending* pending = new Pending(this, callback, client, data);
    cass_future_set_callback(future, on_future_ready, pending);
}

void
AsyncFuture::on_future_ready(CassFuture* future, void* data)
{
    Pending* pending = (Pending*)data;
    pending->owner_->future_ready(future, pending);
}

void
AsyncFuture::future_ready(CassFuture* future, Pending* pending)
{
    uv_mutex_lock(&lock_);
    pending->future_ = future;
    queue_.push(pending);
    uv_mutex_unlock(&lock_);
    uv_async_send(async_);
}

void
AsyncFuture::on_async_ready(uv_async_t* handle, int status)
{
    AsyncFuture* self = (AsyncFuture*)handle->data;
    self->async_ready();
}

uint32_t
timeval_diff_us(const struct timeval &start, const struct timeval &end)
{
    if (start.tv_usec > end.tv_usec) {
        return ((end.tv_sec - 1 - start.tv_sec) * 1000000) +
               ((end.tv_usec + 1000000 - start.tv_usec));
    } else {
        return ((end.tv_sec - start.tv_sec) * 1000000) +
               ((end.tv_usec - start.tv_usec));
    }
}

void
AsyncFuture::async_ready()
{
    uv_mutex_lock(&lock_);

    // Keep track of whether there were leftover items in the ready_queue_ due
    // to the result_loop_elapsed_max limit having been triggered previously. If
    // true, then once the ready_queue is drained, the async needs to be kicked
    // to handle any events that may be in the queue_.
    bool ready_queue_was_empty = ready_queue_.empty();

    if (ready_queue_was_empty) {
        if (queue_.empty()) {
            uv_mutex_unlock(&lock_);
            return;
        }

        std::swap(queue_, ready_queue_);
    }

    uv_mutex_unlock(&lock_);

    uint32_t count = 0;
    struct timeval start, end;
    uint32_t elapsed;

    ::gettimeofday(&start, 0);
    while (! ready_queue_.empty()) {
        Pending* pending = ready_queue_.front();
        ready_queue_.pop();
        pending->callback_(pending->future_, pending->client_, pending->data_);
        delete pending;
        count++;

        if (result_loop_elapsed_max_ != 0) {
            ::gettimeofday(&end, 0);
            elapsed = timeval_diff_us(start, end);
            if (elapsed > result_loop_elapsed_max_ * 1000) {
                metrics_->response_queue_elapsed_limit_count_++;
                break;
            }
        }
    }

    // If there were leftover items in the ready queue when this wakeup
    // occurred, or if there are any leftover items from this iteration, then
    // make sure to schedule another wakeup.
    if (! ready_queue_was_empty || !ready_queue_.empty()) {
        uv_async_send(async_);
    }

    ::gettimeofday(&end, 0);
    elapsed = timeval_diff_us(start, end);

    if (count > metrics_->response_queue_drain_count_max_) {
        metrics_->response_queue_drain_count_max_ = count;
    }

    if (elapsed > metrics_->response_queue_drain_time_max_) {
        metrics_->response_queue_drain_time_max_ = elapsed;
    }
}
