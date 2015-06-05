#include "cassandra.h"
#include <uv.h>
#include <queue>

class Metrics;

// Helper class to schedule a callback on the v8 main thread to handle a
// CassFuture.
class AsyncFuture {
public:
    AsyncFuture(Metrics* metrics);
    ~AsyncFuture();

    // Callback function signature
    typedef void(*callback_t)(CassFuture* future, void* client, void* data);

    // Schedule a callback when the given future is ready.
    void schedule(callback_t callback, CassFuture* future, void* client, void* data);

private:
    // Wrapper for a pending future operation
    struct Pending {
        Pending(AsyncFuture* o, callback_t cb, void* c, void* d)
            : owner_(o), callback_(cb), future_(NULL), client_(c), data_(d) {}

        AsyncFuture* owner_;
        callback_t callback_;
        CassFuture* future_;
        void* client_;
        void* data_;
    };

    Metrics* metrics_;
    uv_mutex_t lock_;
    uv_async_t* async_;
    std::queue<Pending*> queue_;

    // Notification on the worker thread when a future is ready
    static void on_future_ready(CassFuture* future, void* data);
    void future_ready(CassFuture* future, Pending* pending);

    // Notification on the uv main thread when the async has been signaled
#if UV_VERSION_MAJOR == 0
    static void on_async_ready(uv_async_t* handle, int status);
#else
    static void on_async_ready(uv_async_t* handle);
#endif
    void async_ready();
};
