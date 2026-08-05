#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include <boost/asio/buffer.hpp>
#include <boost/beast/core/buffers_cat.hpp>

#include <ws-streaming/detail/dynamic_buffer.hpp>

using namespace testing;
using namespace wss::detail;

namespace
{
    // These helpers deliberately go through the buffer sequence interface instead of assuming the
    // data is contiguous, so that they keep working against an implementation which stores data
    // in separate blocks.

    std::size_t buffered_size(const dynamic_buffer& buffer)
    {
        return boost::asio::buffer_size(buffer.data());
    }

    std::vector<std::uint8_t> contents(const dynamic_buffer& buffer)
    {
        std::vector<std::uint8_t> result(buffered_size(buffer));
        boost::asio::buffer_copy(boost::asio::buffer(result), buffer.data());
        return result;
    }

    std::vector<std::uint8_t> sequence(std::uint8_t first, std::size_t count)
    {
        std::vector<std::uint8_t> result(count);
        for (std::size_t i = 0; i < count; ++i)
            result[i] = static_cast<std::uint8_t>(first + i);
        return result;
    }
}

TEST(DynamicBuffer, StartsEmpty)
{
    dynamic_buffer buffer{16};

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffered_size(buffer), 0u);
}

TEST(DynamicBuffer, ConsumeOnEmptyBufferIsHarmless)
{
    dynamic_buffer buffer{16};

    buffer.consume(0);
    buffer.consume(100);

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffered_size(buffer), 0u);
}

TEST(DynamicBuffer, WritesSingleBuffer)
{
    dynamic_buffer buffer{16};
    auto data = sequence(10, 6);

    EXPECT_EQ(buffer.write(boost::asio::buffer(data)), 6u);
    EXPECT_FALSE(buffer.empty());
    EXPECT_EQ(buffered_size(buffer), 6u);
    EXPECT_EQ(contents(buffer), data);
}

TEST(DynamicBuffer, WritesBufferSequenceWithoutFlattening)
{
    dynamic_buffer buffer{16};
    auto header = sequence(1, 4);
    auto payload = sequence(100, 5);

    EXPECT_EQ(
        buffer.write(
            boost::beast::buffers_cat(
                boost::asio::buffer(header),
                boost::asio::buffer(payload))),
        9u);

    auto expected = header;
    expected.insert(expected.end(), payload.begin(), payload.end());

    EXPECT_EQ(contents(buffer), expected);
}

TEST(DynamicBuffer, WriteOfEmptySequenceChangesNothing)
{
    dynamic_buffer buffer{16};
    auto data = sequence(1, 4);
    buffer.write(boost::asio::buffer(data));

    EXPECT_EQ(buffer.write(boost::asio::const_buffer{}), 0u);
    EXPECT_EQ(contents(buffer), data);
}

TEST(DynamicBuffer, WriteStopsAtTheLimit)
{
    dynamic_buffer buffer{8};
    auto data = sequence(1, 10);

    EXPECT_EQ(buffer.write(boost::asio::buffer(data)), 8u);
    EXPECT_EQ(buffered_size(buffer), 8u);
    EXPECT_EQ(contents(buffer), sequence(1, 8));
}

TEST(DynamicBuffer, WriteToFullBufferAcceptsNothing)
{
    dynamic_buffer buffer{8};
    auto data = sequence(1, 8);
    ASSERT_EQ(buffer.write(boost::asio::buffer(data)), 8u);

    auto more = sequence(200, 4);
    EXPECT_EQ(buffer.write(boost::asio::buffer(more)), 0u);
    EXPECT_EQ(contents(buffer), data);
}

TEST(DynamicBuffer, PartialConsumeKeepsTheRemainder)
{
    dynamic_buffer buffer{16};
    auto data = sequence(1, 10);
    buffer.write(boost::asio::buffer(data));

    buffer.consume(4);

    EXPECT_FALSE(buffer.empty());
    EXPECT_EQ(buffered_size(buffer), 6u);
    EXPECT_EQ(contents(buffer), sequence(5, 6));
}

TEST(DynamicBuffer, ConsumingEverythingEmptiesTheBuffer)
{
    dynamic_buffer buffer{16};
    auto data = sequence(1, 10);
    buffer.write(boost::asio::buffer(data));

    buffer.consume(10);

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffered_size(buffer), 0u);
}

TEST(DynamicBuffer, ConsumingMoreThanBufferedDiscardsEverything)
{
    dynamic_buffer buffer{16};
    auto data = sequence(1, 10);
    buffer.write(boost::asio::buffer(data));

    buffer.consume(1000);

    EXPECT_TRUE(buffer.empty());
}

TEST(DynamicBuffer, ConsumeOfZeroIsANoOp)
{
    dynamic_buffer buffer{16};
    auto data = sequence(1, 10);
    buffer.write(boost::asio::buffer(data));

    buffer.consume(0);

    EXPECT_EQ(contents(buffer), data);
}

TEST(DynamicBuffer, RoomFreedByConsumeIsReusable)
{
    // Storage is reused once a block has been consumed in full: the buffer below holds two
    // blocks, and writing sixteen bytes through it in eight-byte steps only works if the first
    // block comes back around after being emptied.
    dynamic_buffer buffer{16, 8};

    for (std::uint8_t round = 0; round < 4; ++round)
    {
        auto data = sequence(static_cast<std::uint8_t>(10 * round), 8);

        ASSERT_EQ(buffer.write(boost::asio::buffer(data)), 8u);
        EXPECT_EQ(contents(buffer), data);

        buffer.consume(8);
        EXPECT_TRUE(buffer.empty());
    }
}

TEST(DynamicBuffer, WriteSpansSeveralBlocks)
{
    dynamic_buffer buffer{64, 8};
    auto data = sequence(1, 20);

    EXPECT_EQ(buffer.write(boost::asio::buffer(data)), 20u);
    EXPECT_EQ(buffered_size(buffer), 20u);
    EXPECT_EQ(contents(buffer), data);
}

TEST(DynamicBuffer, ConsumeCrossesBlockBoundaries)
{
    dynamic_buffer buffer{64, 8};
    auto data = sequence(1, 20);
    buffer.write(boost::asio::buffer(data));

    // Lands inside the second block.
    buffer.consume(10);
    EXPECT_EQ(contents(buffer), sequence(11, 10));

    // Lands exactly on a block boundary.
    buffer.consume(6);
    EXPECT_EQ(contents(buffer), sequence(17, 4));

    buffer.consume(4);
    EXPECT_TRUE(buffer.empty());
}

TEST(DynamicBuffer, AppendsToThePartiallyFilledLastBlock)
{
    dynamic_buffer buffer{64, 8};

    ASSERT_EQ(buffer.write(boost::asio::buffer(sequence(1, 5))), 5u);
    ASSERT_EQ(buffer.write(boost::asio::buffer(sequence(6, 7))), 7u);

    EXPECT_EQ(contents(buffer), sequence(1, 12));
}

TEST(DynamicBuffer, TailOfABlockKeepsFillingAfterAPartialConsume)
{
    dynamic_buffer buffer{16, 8};

    ASSERT_EQ(buffer.write(boost::asio::buffer(sequence(1, 4))), 4u);
    buffer.consume(2);

    // Four bytes are still free in the tail of the first block, and the second block is whole.
    EXPECT_EQ(buffer.write(boost::asio::buffer(sequence(10, 12))), 12u);
    EXPECT_EQ(buffered_size(buffer), 14u);
}

TEST(DynamicBuffer, ConsumedSpaceReturnsOnlyWhenTheBlockEmpties)
{
    // Space consumed at the front of a block is not handed out again until that block empties,
    // so a single block filled to its end accepts nothing more while it still holds data.
    dynamic_buffer buffer{8, 8};
    ASSERT_EQ(buffer.write(boost::asio::buffer(sequence(1, 8))), 8u);

    buffer.consume(5);

    EXPECT_EQ(buffer.write(boost::asio::buffer(sequence(100, 3))), 0u);
    EXPECT_EQ(contents(buffer), sequence(6, 3));

    // Once it empties it is recycled and takes data again.
    buffer.consume(3);
    EXPECT_EQ(buffer.write(boost::asio::buffer(sequence(100, 8))), 8u);
}

TEST(DynamicBuffer, AllocatesNoMoreBlocksThanTheLimitAllows)
{
    dynamic_buffer buffer{64, 8};
    auto data = sequence(1, 100);

    EXPECT_EQ(buffer.write(boost::asio::buffer(data)), 64u);
    EXPECT_EQ(buffered_size(buffer), 64u);
    EXPECT_EQ(buffer.write(boost::asio::buffer(sequence(1, 8))), 0u);
}

TEST(DynamicBuffer, LimitNotDivisibleByBlockSizeRoundsDown)
{
    // Twelve eight-byte blocks fit into 100 bytes; the leftover four bytes are unusable.
    dynamic_buffer buffer{100, 8};

    EXPECT_EQ(buffer.write(boost::asio::buffer(sequence(1, 100))), 96u);
}

TEST(DynamicBuffer, BlockSizeLargerThanTheLimitIsClamped)
{
    dynamic_buffer buffer{10, 64 * 1024};

    EXPECT_EQ(buffer.write(boost::asio::buffer(sequence(1, 20))), 10u);
    EXPECT_EQ(contents(buffer), sequence(1, 10));
}

TEST(DynamicBuffer, DataOnEmptyBufferIsAnEmptySequence)
{
    dynamic_buffer buffer{64, 8};

    EXPECT_EQ(boost::asio::buffer_size(buffer.data()), 0u);

    buffer.write(boost::asio::buffer(sequence(1, 20)));
    buffer.consume(20);

    EXPECT_EQ(boost::asio::buffer_size(buffer.data()), 0u);
}

TEST(DynamicBuffer, ReusesBlocksAcrossFarMoreDataThanItHolds)
{
    dynamic_buffer buffer{64, 8};

    std::vector<std::uint8_t> written;
    std::vector<std::uint8_t> read;
    std::uint8_t next_value = 0;

    // Pushes several hundred bytes through a 64-byte buffer, which is only possible if emptied
    // blocks are returned to the pool and filled again.
    for (int i = 0; i < 100; ++i)
    {
        auto chunk = sequence(next_value, 1 + (i % 23));
        auto accepted = buffer.write(boost::asio::buffer(chunk));

        written.insert(written.end(), chunk.begin(), chunk.begin() + accepted);
        next_value = static_cast<std::uint8_t>(next_value + accepted);

        auto buffered = contents(buffer);
        auto taken = std::min<std::size_t>(buffered.size(), 1 + (i % 19));

        read.insert(read.end(), buffered.begin(), buffered.begin() + taken);
        buffer.consume(taken);
    }

    auto remaining = contents(buffer);
    read.insert(read.end(), remaining.begin(), remaining.end());
    buffer.consume(remaining.size());

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(read, written);

    // Many times what the buffer can hold at once.
    EXPECT_GT(written.size(), 10 * 64u);
}

TEST(DynamicBuffer, PreservesOrderAcrossManyWriteConsumeCycles)
{
    dynamic_buffer buffer{64};

    std::vector<std::uint8_t> written;
    std::vector<std::uint8_t> read;
    std::uint8_t next_value = 0;

    for (int i = 0; i < 200; ++i)
    {
        auto chunk = sequence(next_value, 1 + (i % 17));
        auto accepted = buffer.write(boost::asio::buffer(chunk));

        written.insert(written.end(), chunk.begin(), chunk.begin() + accepted);
        next_value = static_cast<std::uint8_t>(next_value + accepted);

        auto buffered = contents(buffer);
        auto taken = std::min<std::size_t>(buffered.size(), 1 + (i % 11));

        read.insert(read.end(), buffered.begin(), buffered.begin() + taken);
        buffer.consume(taken);
    }

    auto remaining = contents(buffer);
    read.insert(read.end(), remaining.begin(), remaining.end());
    buffer.consume(remaining.size());

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(read, written);
}
