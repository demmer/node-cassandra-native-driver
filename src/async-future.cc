#include "async-future.h"

AsyncFuture::AsyncFuture()
{
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

void
AsyncFuture::async_ready()
{
    do {
        uv_mutex_lock(&lock_);
        if (queue_.empty()) {
            uv_mutex_unlock(&lock_);
            break;
        }
        Pending* pending = queue_.front();
        queue_.pop();
        uv_mutex_unlock(&lock_);

        pending->callback_(pending->future_, pending->client_, pending->data_);
        delete pending;
    } while (true);
}
