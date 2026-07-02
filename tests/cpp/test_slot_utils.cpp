#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/value_slot_store.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

struct TrackedKey
{
    static inline int live_instances{0};
    static inline int copied{0};

    int value{0};

    explicit TrackedKey(int value_) : value(value_) { ++live_instances; }

    TrackedKey(const TrackedKey &other) : value(other.value) {
        ++live_instances;
        ++copied;
    }

    TrackedKey(TrackedKey &&other) noexcept : value(other.value) { ++live_instances; }

    ~TrackedKey() { --live_instances; }

    [[nodiscard]] bool operator==(const TrackedKey &other) const noexcept { return value == other.value; }

    static void reset() {
        live_instances = 0;
        copied         = 0;
    }
};

namespace std
{
    template <> struct hash<TrackedKey>
    {
        [[nodiscard]] size_t operator()(const TrackedKey &key) const noexcept { return hash<int>{}(key.value); }
    };
}  // namespace std

namespace
{
    using namespace hgraph;

    struct AllocationProbe
    {
        static inline int                                     allocations{0};
        static inline int                                     deallocations{0};
        static inline std::vector<MemoryUtils::StorageLayout> allocated_layouts{};
        static inline std::vector<MemoryUtils::StorageLayout> deallocated_layouts{};

        static void reset() {
            allocations   = 0;
            deallocations = 0;
            allocated_layouts.clear();
            deallocated_layouts.clear();
        }
    };

    void *tracked_allocate(MemoryUtils::StorageLayout layout) {
        ++AllocationProbe::allocations;
        AllocationProbe::allocated_layouts.push_back(layout);
        return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
    }

    void tracked_deallocate(void *memory, MemoryUtils::StorageLayout layout) noexcept {
        ++AllocationProbe::deallocations;
        AllocationProbe::deallocated_layouts.push_back(layout);
        ::operator delete(memory, std::align_val_t{layout.alignment});
    }

    struct RecordingObserver final : SlotObserver
    {
        std::vector<std::string> events{};

        void on_capacity(size_t old_capacity, size_t new_capacity) override {
            events.push_back("capacity:" + std::to_string(old_capacity) + "->" + std::to_string(new_capacity));
        }

        void on_insert(size_t slot) override { events.push_back("insert:" + std::to_string(slot)); }

        void on_remove(size_t slot) override { events.push_back("remove:" + std::to_string(slot)); }

        void on_erase(size_t slot) override { events.push_back("erase:" + std::to_string(slot)); }

        void on_clear() override { events.push_back("clear"); }
    };

    struct SelfRemovingSlotObserver final : SlotObserver
    {
        SlotObserverList *list{nullptr};
        int               calls{0};

        explicit SelfRemovingSlotObserver(SlotObserverList *list_) : list(list_) {}

        void on_capacity(size_t, size_t) override {}

        void on_insert(size_t) override
        {
            ++calls;
            list->remove(this);
        }

        void on_remove(size_t) override {}
        void on_erase(size_t) override {}
        void on_clear() override {}
    };

    struct TrackedPayload
    {
        static inline int constructed{0};
        static inline int destroyed{0};

        int value{0};

        explicit TrackedPayload(int value_) : value(value_) { ++constructed; }

        TrackedPayload(const TrackedPayload &other) : value(other.value) { ++constructed; }

        TrackedPayload &operator=(const TrackedPayload &) = default;
        TrackedPayload &operator=(TrackedPayload &&)      = default;

        ~TrackedPayload() { ++destroyed; }

        static void reset() {
            constructed = 0;
            destroyed   = 0;
        }
    };
}  // namespace

TEST_CASE("stable slot storage preserves existing slot addresses across chained growth", "[v2 slot utils]") {
    StableSlotStorage storage;

    storage.reserve_to(2, sizeof(std::int64_t), alignof(std::int64_t));
    REQUIRE(storage.slot_capacity() == 2);
    REQUIRE(storage.slot_data(0) != nullptr);
    REQUIRE(storage.slot_data(1) != nullptr);

    void *slot0 = storage.slot_data(0);
    void *slot1 = storage.slot_data(1);

    CHECK(reinterpret_cast<std::uintptr_t>(slot0) % alignof(std::int64_t) == 0U);
    CHECK(reinterpret_cast<std::uintptr_t>(slot1) % alignof(std::int64_t) == 0U);

    storage.reserve_to(8, sizeof(std::int64_t), alignof(std::int64_t));
    CHECK(storage.slot_capacity() == 8);
    CHECK(storage.slot_data(0) == slot0);
    CHECK(storage.slot_data(1) == slot1);

    REQUIRE(storage.slot_data(7) != nullptr);
    CHECK(storage.slot_data(7) != slot0);
    CHECK(storage.slot_data(7) != slot1);
}

TEST_CASE("stable slot storage rejects layout changes after binding", "[v2 slot utils]") {
    StableSlotStorage storage;
    storage.reserve_to(4, sizeof(std::uint32_t), alignof(std::uint32_t));

    REQUIRE_THROWS_AS(storage.reserve_to(8, sizeof(std::uint64_t), alignof(std::uint64_t)), std::logic_error);
}

TEST_CASE("stable slot storage allocates blocks through allocator ops", "[v2 slot utils]") {
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        StableSlotStorage storage(allocator);
        storage.reserve_to(2, sizeof(std::uint32_t), alignof(std::uint32_t));
        storage.reserve_to(5, sizeof(std::uint32_t), alignof(std::uint32_t));

        REQUIRE(AllocationProbe::allocations == 2);
        REQUIRE(AllocationProbe::allocated_layouts.size() == 2);
        CHECK(AllocationProbe::allocated_layouts[0].size == sizeof(std::uint32_t) * 2);
        CHECK(AllocationProbe::allocated_layouts[0].alignment == alignof(std::uint32_t));
        CHECK(AllocationProbe::allocated_layouts[1].size == sizeof(std::uint32_t) * 3);
        CHECK(AllocationProbe::allocated_layouts[1].alignment == alignof(std::uint32_t));
        CHECK(&storage.allocator() == &allocator);
    }

    REQUIRE(AllocationProbe::deallocations == 2);
    REQUIRE(AllocationProbe::deallocated_layouts.size() == AllocationProbe::allocated_layouts.size());
    CHECK(AllocationProbe::deallocated_layouts[0].size + AllocationProbe::deallocated_layouts[1].size ==
          AllocationProbe::allocated_layouts[0].size + AllocationProbe::allocated_layouts[1].size);
    CHECK(AllocationProbe::deallocated_layouts[0].alignment == alignof(std::uint32_t));
    CHECK(AllocationProbe::deallocated_layouts[1].alignment == alignof(std::uint32_t));
}

TEST_CASE("value slot store tracks updates and notifies observers", "[v2 slot utils]") {
    ValueSlotStore    store(MemoryUtils::plan_for<std::uint32_t>());
    RecordingObserver observer;

    store.add_slot_observer(&observer);
    store.reserve_to(4);
    store.notify_capacity(0, store.slot_capacity());
    store.notify_insert(1);

    REQUIRE_FALSE(store.slot_updated(1));
    REQUIRE_FALSE(store.has_slot(1));
    store.mark_updated(1);
    REQUIRE(store.slot_updated(1));
    store.clear_updated(1);
    REQUIRE_FALSE(store.slot_updated(1));

    store.notify_remove(1);
    store.notify_erase(1);
    store.notify_clear();

    CHECK(observer.events == std::vector<std::string>{"capacity:0->4", "insert:1", "remove:1", "erase:1", "clear"});

    store.remove_slot_observer(&observer);
    store.notify_insert(2);
    CHECK(observer.events.size() == 5);
}

TEST_CASE("slot observer list supports reentrant removal", "[v2 slot utils]") {
    SlotObserverList         observers;
    SelfRemovingSlotObserver first{&observers};
    RecordingObserver        second;

    observers.add(&first);
    observers.add(&second);

    observers.notify_insert(3);
    REQUIRE(first.calls == 1);
    REQUIRE(second.events == std::vector<std::string>{"insert:3"});

    observers.notify_insert(4);
    REQUIRE(first.calls == 1);
    REQUIRE(second.events == std::vector<std::string>{"insert:3", "insert:4"});
}

TEST_CASE("value slot store supports default construction before plan binding", "[v2 slot utils]") {
    ValueSlotStore store;

    REQUIRE(store.plan() == nullptr);
    REQUIRE_THROWS_AS(store.reserve_to(1), std::logic_error);

    const auto &int_plan = MemoryUtils::plan_for<std::uint32_t>();
    store.bind_plan(int_plan);
    REQUIRE(store.plan() == &int_plan);

    store.reserve_to(2);
    store.construct_at(1);
    REQUIRE(store.has_slot(1));
    store.destroy_at(1);

    store.bind_plan(int_plan);
    REQUIRE_THROWS_AS(store.bind_plan(MemoryUtils::plan_for<std::uint64_t>()), std::logic_error);
}

TEST_CASE("value slot store manages typed payload lifetime on stable slots", "[v2 slot utils]") {
    TrackedPayload::reset();

    ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
    store.reserve_to(2);

    void *slot0 = store.value_memory(0);
    REQUIRE(slot0 != nullptr);

    auto &first = store.construct_at<TrackedPayload>(0, 11);
    REQUIRE(&first == store.try_value<TrackedPayload>(0));
    REQUIRE(first.value == 11);
    REQUIRE(store.has_slot(0));
    REQUIRE(TrackedPayload::constructed == 1);

    store.reserve_to(6);
    CHECK(store.value_memory(0) == slot0);

    auto &second = store.construct_at<TrackedPayload>(5, 29);
    REQUIRE(&second == store.try_value<TrackedPayload>(5));
    REQUIRE(second.value == 29);
    REQUIRE(TrackedPayload::constructed == 2);

    store.destroy_at(0);
    REQUIRE_FALSE(store.has_slot(0));
    REQUIRE(TrackedPayload::destroyed == 1);

    store.destroy_all();
    REQUIRE_FALSE(store.has_slot(5));
    REQUIRE(TrackedPayload::destroyed == 2);
}

TEST_CASE("value slot store destroys live payloads on scope exit", "[v2 slot utils]") {
    TrackedPayload::reset();

    {
        ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
        store.reserve_to(2);
        store.construct_at<TrackedPayload>(0, 7);
        store.construct_at<TrackedPayload>(1, 13);

        REQUIRE(TrackedPayload::constructed == 2);
        REQUIRE(TrackedPayload::destroyed == 0);
    }

    REQUIRE(TrackedPayload::destroyed == 2);
}

TEST_CASE("value slot store rejects invalid emplacement", "[v2 slot utils]") {
    TrackedPayload::reset();

    ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>());
    store.reserve_to(1);

    store.construct_at<TrackedPayload>(0, 5);
    REQUIRE_THROWS_AS(store.construct_at<TrackedPayload>(0, 9), std::logic_error);
    REQUIRE_THROWS_AS(store.construct_at<TrackedPayload>(1, 11), std::out_of_range);
    REQUIRE_THROWS_AS(store.construct_at<std::string>(0, "wrong plan"), std::logic_error);
}

TEST_CASE("value slot store uses its bound plan for lifecycle operations", "[v2 slot utils]") {
    ValueSlotStore store(MemoryUtils::plan_for<std::string>());
    store.reserve_to(2);

    REQUIRE(store.plan() == &MemoryUtils::plan_for<std::string>());

    store.construct_at(0);
    REQUIRE(store.has_slot(0));
    REQUIRE(*store.try_value<std::string>(0) == "");

    const std::string source = "copied";
    store.construct_at(1, &source);
    REQUIRE(store.has_slot(1));
    REQUIRE(*store.try_value<std::string>(1) == "copied");

    store.destroy_all();
    REQUIRE_FALSE(store.has_slot(0));
    REQUIRE_FALSE(store.has_slot(1));
}

TEST_CASE("value slot stores can share a custom allocator", "[v2 slot utils]") {
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        ValueSlotStore store(MemoryUtils::plan_for<TrackedPayload>(), allocator);
        store.reserve_to(3);
        store.construct_at<TrackedPayload>(1, 17);
        REQUIRE(&store.value_storage.allocator() == &allocator);
        REQUIRE(AllocationProbe::allocations == 1);
    }

    REQUIRE(AllocationProbe::deallocations == 1);
}

TEST_CASE("key mirrored value slot store derives lifetime from key construction", "[v2 slot utils]") {
    KeySlotStore             keys(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());
    KeyMirroredValueSlotStore values(keys, MemoryUtils::plan_for<std::string>());

    REQUIRE(values.mirrors_key_construction());

    const auto first = keys.insert(11);
    REQUIRE(first.inserted);
    REQUIRE(values.has_slot(first.slot));
    REQUIRE(values.mirrors_key_construction());

    auto *value = values.try_value<std::string>(first.slot);
    REQUIRE(value != nullptr);
    *value = "eleven";
    values.mark_updated(first.slot);
    REQUIRE(values.slot_updated(first.slot));

    REQUIRE(keys.remove(11));
    REQUIRE(keys.slot_constructed(first.slot));
    REQUIRE_FALSE(keys.slot_live(first.slot));
    REQUIRE(values.has_slot(first.slot));
    REQUIRE(values.try_value<std::string>(first.slot) != nullptr);
    REQUIRE(*values.try_value<std::string>(first.slot) == "eleven");
    REQUIRE(values.mirrors_key_construction());

    keys.erase_pending();
    REQUIRE_FALSE(keys.slot_constructed(first.slot));
    REQUIRE_FALSE(values.has_slot(first.slot));
    REQUIRE(values.try_value<std::string>(first.slot) == nullptr);
    REQUIRE(values.mirrors_key_construction());
}

TEST_CASE("key slot store tracks pending removals until explicit erase", "[v2 slot utils]") {
    KeySlotStore      store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());
    RecordingObserver observer;

    store.add_slot_observer(&observer);
    store.reserve_to<std::int32_t>(4);

    const auto first  = store.insert(11);
    const auto second = store.insert(22);

    REQUIRE(first.inserted);
    REQUIRE(second.inserted);
    REQUIRE(first.slot == 0);
    REQUIRE(second.slot == 1);
    REQUIRE(store.contains(11));
    REQUIRE(store.find_slot(22) == second.slot);
    REQUIRE(store.find_stored_slot(22) == second.slot);
    REQUIRE(store.try_key<std::int32_t>(first.slot) != nullptr);
    REQUIRE(*store.try_key<std::int32_t>(first.slot) == 11);

    REQUIRE(store.remove(22));
    REQUIRE(store.size() == 1);
    REQUIRE_FALSE(store.slot_live(second.slot));
    REQUIRE(store.slot_constructed(second.slot));
    REQUIRE(store.slot_pending_erase(second.slot));
    REQUIRE(store.pending_erase_count() == 1);
    REQUIRE(store.has_pending_erase());
    REQUIRE(store.find_stored_slot(22) == second.slot);
    REQUIRE(store.find_slot(22) == KeySlotStore::npos);
    REQUIRE_FALSE(store.contains(22));
    REQUIRE(store.try_key<std::int32_t>(second.slot) != nullptr);
    REQUIRE(*store.try_key<std::int32_t>(second.slot) == 22);

    CHECK(observer.events == std::vector<std::string>{"capacity:0->4", "insert:0", "insert:1", "remove:1"});

    REQUIRE_FALSE(store.slot_live(second.slot));
    REQUIRE(store.slot_constructed(second.slot));
    REQUIRE(store.slot_pending_erase(second.slot));
    REQUIRE(store.has_pending_erase());
    REQUIRE(store.pending_erase_count() == 1);
    REQUIRE(store.find_stored_slot(22) == second.slot);
    REQUIRE(store.try_key<std::int32_t>(second.slot) != nullptr);

    store.erase_pending();

    REQUIRE_FALSE(store.slot_live(second.slot));
    REQUIRE_FALSE(store.slot_constructed(second.slot));
    REQUIRE_FALSE(store.slot_pending_erase(second.slot));
    REQUIRE_FALSE(store.has_pending_erase());
    REQUIRE(store.pending_erase_count() == 0);
    REQUIRE(store.find_stored_slot(22) == KeySlotStore::npos);
    REQUIRE(store.try_key<std::int32_t>(second.slot) == nullptr);

    CHECK(observer.events == std::vector<std::string>{"capacity:0->4", "insert:0", "insert:1", "remove:1", "erase:1"});
}

TEST_CASE("key slot store resurrects removed keys before erase and reuses slots after explicit erase", "[v2 slot utils]") {
    KeySlotStore store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());

    const auto original = store.insert(3);
    REQUIRE(original.inserted);

    REQUIRE(store.remove(3));
    REQUIRE_FALSE(store.contains(3));
    REQUIRE_FALSE(store.slot_live(original.slot));
    REQUIRE(store.slot_constructed(original.slot));
    REQUIRE(store.slot_pending_erase(original.slot));
    REQUIRE(store.pending_erase_count() == 1);

    const auto resurrected = store.insert(3);
    REQUIRE(resurrected.inserted);
    REQUIRE(resurrected.slot == original.slot);
    REQUIRE(store.contains(3));
    REQUIRE(store.slot_live(original.slot));
    REQUIRE(store.slot_constructed(original.slot));
    REQUIRE_FALSE(store.slot_pending_erase(original.slot));
    REQUIRE(store.pending_erase_count() == 0);

    REQUIRE(store.remove(3));
    REQUIRE(store.slot_pending_erase(original.slot));

    store.erase_pending();
    REQUIRE_FALSE(store.slot_live(original.slot));
    REQUIRE_FALSE(store.slot_constructed(original.slot));
    REQUIRE_FALSE(store.has_pending_erase());

    const auto reused = store.insert(9);
    REQUIRE(reused.inserted);
    REQUIRE(reused.slot == original.slot);
    REQUIRE(store.contains(9));
    REQUIRE(*store.try_key<std::int32_t>(reused.slot) == 9);
}

TEST_CASE("key slot store bracket accessor reads constructed slots and throws after erase", "[v2 slot utils]") {
    KeySlotStore store(MemoryUtils::plan_for<std::int32_t>(), key_slot_store_ops_for<std::int32_t>());

    const auto inserted = store.insert(7);
    REQUIRE(*MemoryUtils::cast<int>(store[inserted.slot]) == 7);

    REQUIRE(store.remove(7));
    REQUIRE_FALSE(store.slot_live(inserted.slot));
    REQUIRE(store.slot_constructed(inserted.slot));
    REQUIRE(*MemoryUtils::cast<int>(store[inserted.slot]) == 7);

    store.erase_pending();
    REQUIRE_FALSE(store.slot_constructed(inserted.slot));
    REQUIRE_THROWS_AS(static_cast<void>(store[inserted.slot]), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(store[store.slot_capacity()]), std::out_of_range);
}

TEST_CASE("key slot store supports custom allocators with explicit pending erase", "[v2 slot utils]") {
    TrackedKey::reset();
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        KeySlotStore      store(MemoryUtils::plan_for<TrackedKey>(), key_slot_store_ops_for<TrackedKey>(), allocator);
        RecordingObserver observer;
        store.add_slot_observer(&observer);
        store.reserve_to<TrackedKey>(2);

        TrackedKey alpha{11};
        REQUIRE(TrackedKey::live_instances == 1);

        const auto inserted = store.insert(alpha);
        REQUIRE(inserted.inserted);
        REQUIRE(TrackedKey::copied == 1);
        REQUIRE(TrackedKey::live_instances == 2);

        REQUIRE(store.remove(alpha));
        REQUIRE_FALSE(store.slot_live(inserted.slot));
        REQUIRE(store.slot_constructed(inserted.slot));
        REQUIRE(store.slot_pending_erase(inserted.slot));
        REQUIRE(store.find_stored_slot(alpha) == inserted.slot);
        REQUIRE(TrackedKey::live_instances == 2);

        store.erase_pending();
        REQUIRE_FALSE(store.slot_live(inserted.slot));
        REQUIRE_FALSE(store.slot_constructed(inserted.slot));
        REQUIRE(store.find_stored_slot(alpha) == KeySlotStore::npos);
        REQUIRE(TrackedKey::live_instances == 1);

        TrackedKey beta{29};
        const auto reused = store.insert(beta);
        REQUIRE(reused.inserted);
        REQUIRE(reused.slot == inserted.slot);
        REQUIRE(TrackedKey::copied == 2);
        REQUIRE(&store.allocator() == &allocator);
        REQUIRE(AllocationProbe::allocations == 1);

        store.clear();
        REQUIRE(TrackedKey::live_instances == 2);

        CHECK(observer.events == std::vector<std::string>{"capacity:0->2", "insert:0", "remove:0", "erase:0", "insert:0", "clear"});
    }

    REQUIRE(TrackedKey::live_instances == 0);
    REQUIRE(AllocationProbe::deallocations == 1);
}
