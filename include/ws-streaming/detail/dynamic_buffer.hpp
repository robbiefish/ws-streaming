#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <vector>

#include <boost/asio/buffer.hpp>

namespace wss::detail
{
    /**
     * Buffers outgoing data which the operating system was not able to accept immediately. Data
     * is appended with write() and stays queued until the socket becomes writeable, at which
     * point the caller passes data() to a send operation and reports the accepted byte count back
     * with consume(). Data is always returned in the order it was written.
     *
     * Data is stored in fixed-size blocks which are allocated on demand, up to a total allocation
     * limit given to the constructor, and are never moved or copied afterwards. A block emptied
     * by consume() goes to the back of the queue and is filled again later, so a connection which
     * repeatedly falls behind and catches up settles at the number of blocks its backlog actually
     * needs. Because the contents are spread over several blocks, data() describes them as a
     * sequence of buffers, which a send operation transfers in a single scatter-gather call.
     *
     * Callers detect the allocation limit through the return value of write(), which reports how
     * many bytes were actually accepted; a caller which cannot tolerate partial writes should
     * treat a short count as a fatal condition for the connection. Note that while the tail of
     * the last block keeps filling, space already consumed at the front of a block returns to use
     * only once that block empties, so the amount that fits can be below the nominal limit by up
     * to one block.
     *
     * Blocks are not released until the buffer is destroyed.
     *
     * This class is not thread-safe. Callers must not invoke member functions concurrently.
     */
    class dynamic_buffer
    {
        public:

            /**
             * A view over an array of Boost.Asio buffer descriptors, satisfying the Boost.Asio
             * requirements for a buffer sequence.
             *
             * @tparam Buffer The Boost.Asio buffer type described by the sequence.
             */
            template <typename Buffer>
            class buffer_span
            {
                public:

                    using value_type = Buffer;              /**< The buffer type. */
                    using const_iterator = const Buffer *;  /**< The sequence's iterator type. */

                    buffer_span() = default;

                    /**
                     * Constructs a view over the specified range of buffer descriptors.
                     *
                     * @param begin A pointer to the first descriptor.
                     * @param end A pointer past the last descriptor.
                     */
                    buffer_span(const Buffer *begin, const Buffer *end) noexcept
                        : _begin(begin)
                        , _end(end)
                    {
                    }

                    const_iterator begin() const noexcept { return _begin; }
                    const_iterator end() const noexcept { return _end; }

                private:

                    const Buffer *_begin = nullptr;
                    const Buffer *_end = nullptr;
            };

            /**
             * The type returned by data(): a sequence of one buffer per block holding data.
             * Callers must treat it only as a Boost.Asio constant buffer sequence.
             */
            using const_buffers_type = buffer_span<boost::asio::const_buffer>;

            /**
             * Constructs an empty buffer. No blocks are allocated until data is first written.
             *
             * @param max_size The maximum number of bytes to allocate for blocks. This bounds the
             *     number of blocks; because a partially consumed block is not refilled, slightly
             *     less than this may be storable at any one moment.
             * @param block_size The size of an individual block. Values larger than @p max_size
             *     are reduced to it.
             */
            explicit dynamic_buffer(
                std::size_t max_size,
                std::size_t block_size = 64 * 1024);

            /**
             * Appends data to the buffer, copying as much of the specified buffer sequence as
             * fits. This function directly supports scatter-gather operations: the caller can
             * pass a concatenated sequence, such as a packet header followed by its payload,
             * without flattening it first. Data may span several blocks.
             *
             * @perfcrit This function is called once for every transmitted packet which cannot be
             *     sent synchronously.
             *
             * @tparam ConstBufferSequence A type that satisfies the Boost.Asio requirements for a
             *     sequence of immutable buffers.
             *
             * @param buffers A sequence of Boost.Asio buffer descriptors for the data to append.
             *
             * @return The number of bytes appended, which is less than the size of @p buffers if
             *     the buffer ran out of room.
             */
            template <typename ConstBufferSequence>
            std::size_t write(const ConstBufferSequence& buffers)
            {
                std::size_t bytes_written = boost::asio::buffer_copy(
                    prepare(boost::asio::buffer_size(buffers)),
                    buffers);

                commit(bytes_written);

                return bytes_written;
            }

            /**
             * Gets the buffered data, oldest first, as a Boost.Asio constant buffer sequence
             * suitable for passing directly to a send operation. The sequence describes at most
             * MAX_BUFFERS blocks; anything beyond that is returned by a later call, once the
             * described data has been consumed.
             *
             * @perfcrit This function is called once for every send operation.
             *
             * @return A constant buffer sequence describing the buffered data. The sequence
             *     remains valid until the next call to write() or consume().
             */
            const_buffers_type data() const;

            /**
             * Discards the specified number of bytes from the front of the buffer, after they
             * have been accepted by a send operation. Blocks emptied by this call are queued for
             * reuse rather than released.
             *
             * @perfcrit This function is called once for every send operation.
             *
             * @param size The number of bytes to discard. Values larger than the amount of
             *     buffered data discard everything.
             */
            void consume(std::size_t size);

            /**
             * Checks whether any data is currently buffered.
             *
             * @return True if the buffer holds no data.
             */
            bool empty() const noexcept
            {
                return _size == 0;
            }

        private:

            /**
             * The largest number of buffer descriptors data() will return. Boost.Asio's own
             * adapter passes no more than this to the operating system, so describing further
             * blocks would be wasted work.
             */
            static constexpr std::size_t MAX_BUFFERS = 64;

            using mutable_buffers_type = buffer_span<boost::asio::mutable_buffer>;

            mutable_buffers_type prepare(std::size_t size);
            void commit(std::size_t size) noexcept;

            void recycle_front_block();

            // The blocks holding data are the first _used blocks of _blocks; the rest are empty
            // and available. Data starts at _read_offset in the first block and ends at
            // _write_offset in the last one. When _used is zero the buffer is empty and both
            // offsets are zero.
            std::deque<std::vector<std::uint8_t>> _blocks;

            std::size_t _block_size;
            std::size_t _max_blocks;

            std::size_t _used = 0;
            std::size_t _read_offset = 0;
            std::size_t _write_offset = 0;
            std::size_t _size = 0;

            mutable std::vector<boost::asio::const_buffer> _gather;
            std::vector<boost::asio::mutable_buffer> _scatter;
    };
}
