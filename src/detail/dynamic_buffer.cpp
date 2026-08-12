#include <algorithm>
#include <cstddef>
#include <utility>

#include <ws-streaming/detail/dynamic_buffer.hpp>

wss::detail::dynamic_buffer::dynamic_buffer(
        std::size_t max_size,
        std::size_t block_size)
    : _block_size(std::min(block_size, max_size))
    , _max_blocks(_block_size ? max_size / _block_size : 0)
{
}

wss::detail::dynamic_buffer::const_buffers_type
wss::detail::dynamic_buffer::data() const
{
    _gather.clear();

    for (std::size_t i = 0; i < _used && _gather.size() < MAX_BUFFERS; ++i)
    {
        std::size_t begin = i ? 0 : _read_offset;
        std::size_t end = (i == _used - 1) ? _write_offset : _block_size;

        if (end > begin)
            _gather.emplace_back(_blocks[i].data() + begin, end - begin);
    }

    return {_gather.data(), _gather.data() + _gather.size()};
}

void wss::detail::dynamic_buffer::consume(std::size_t size)
{
    size = std::min(size, _size);
    _size -= size;

    while (size)
    {
        // Only the last occupied block is partially filled; all the others run to their end.
        std::size_t end = (_used == 1) ? _write_offset : _block_size;
        std::size_t bytes_consumed = std::min(size, end - _read_offset);

        _read_offset += bytes_consumed;
        size -= bytes_consumed;

        if (_read_offset == end)
            recycle_front_block();
    }
}

wss::detail::dynamic_buffer::mutable_buffers_type
wss::detail::dynamic_buffer::prepare(std::size_t size)
{
    _scatter.clear();

    // Fill the tail of the last occupied block before taking any further block.
    if (size && _used && _write_offset < _block_size)
    {
        std::size_t available = std::min(size, _block_size - _write_offset);

        _scatter.emplace_back(_blocks[_used - 1].data() + _write_offset, available);
        size -= available;
    }

    // Then take blocks which were emptied earlier, allocating new ones while the limit allows.
    for (std::size_t i = _used; size; ++i)
    {
        if (i == _blocks.size())
        {
            if (_blocks.size() == _max_blocks)
                break;

            _blocks.emplace_back(_block_size);
        }

        std::size_t available = std::min(size, _block_size);

        _scatter.emplace_back(_blocks[i].data(), available);
        size -= available;
    }

    return {_scatter.data(), _scatter.data() + _scatter.size()};
}

void wss::detail::dynamic_buffer::commit(std::size_t size) noexcept
{
    _size += size;

    // The tail of the last occupied block was filled first.
    if (_used && _write_offset < _block_size)
    {
        std::size_t bytes_committed = std::min(size, _block_size - _write_offset);

        _write_offset += bytes_committed;
        size -= bytes_committed;
    }

    // Everything beyond that occupies further blocks, the last of which may be partially filled.
    while (size)
    {
        ++_used;
        _write_offset = std::min(size, _block_size);
        size -= _write_offset;
    }
}

void wss::detail::dynamic_buffer::recycle_front_block()
{
    // Instead of releasing a block, make it the last of the available blocks.
    _blocks.push_back(std::move(_blocks.front()));
    _blocks.pop_front();

    --_used;
    _read_offset = 0;

    if (!_used)
        _write_offset = 0;
}
