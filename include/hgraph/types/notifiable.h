#ifndef HGRAPH_CPP_NOTIFIABLE_H
#define HGRAPH_CPP_NOTIFIABLE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{
    /**
     * Runtime notification target used by time-series observers.
     *
     * Time-series data does not own observers. A subscriber registers a
     * ``Notifiable`` pointer at the level it wants to observe and remains
     * responsible for unregistering before it is destroyed. ``notify`` is
     * called once per observed level and engine time when the level first
     * records a modification.
     */
    struct HGRAPH_EXPORT Notifiable
    {
        virtual ~Notifiable() = default;

        /** Receive a time-series modification notification. */
        virtual void notify(engine_time_t modified_time) = 0;

        /**
         * Called when the observed storage is destroyed while this observer is
         * still registered. Inputs can use this hook to clear borrowed source
         * pointers. The default is a no-op for observers that do not retain the
         * observed address.
         */
        virtual void notify_invalidated() noexcept {}
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_NOTIFIABLE_H
