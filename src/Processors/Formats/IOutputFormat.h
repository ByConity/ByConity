#pragma once

#include <string>
#include <IO/OutfileCommon.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <IO/Progress.h>


namespace DB
{

class WriteBuffer;
class OutfileTarget;
using OutfileTargetPtr = std::shared_ptr<OutfileTarget>;
class MPPQueryCoordinator;
using MPPQueryCoordinatorPtr = std::shared_ptr<MPPQueryCoordinator>;

/** Output format have three inputs and no outputs. It writes data from WriteBuffer.
  *
  * First input is for main resultset, second is for "totals" and third is for "extremes".
  * It's not necessarily to connect "totals" or "extremes" ports (they may remain dangling).
  *
  * Data from input ports are pulled in order: first, from main input, then totals, then extremes.
  *
  * By default, data for "totals" and "extremes" is ignored.
  */
class IOutputFormat : public IProcessor
{
public:
    enum PortKind { Main = 0, Totals = 1, Extremes = 2 };

protected:
    WriteBuffer & out;

    /// Used when write select result into directory, and data will be split into sever segments
    /// Also used to ensure out buffer's life cycle is as long as this class
    OutfileTargetPtr outfile_target;

    Chunk current_chunk;
    PortKind current_block_kind = PortKind::Main;
    bool has_input = false;
    bool finished = false;
    bool finalized = false;

    /// Flush data on each consumed chunk. This is intended for interactive applications to output data as soon as it's ready.
    bool auto_flush = false;

    RowsBeforeLimitCounterPtr rows_before_limit_counter;
    // for output query duration
    Stopwatch watch;

    friend class ParallelFormattingOutputFormat;

    virtual void consume(Chunk) = 0;
    virtual void consumeTotals(Chunk) {}
    virtual void consumeExtremes(Chunk) {}
    virtual void finalize() {}

    /// for OutputFormat implementation like 'Parquet', there is a
    /// inner file descriptor, release the file descriptor or the WriteBuffer by self
    /// only used when 'INTO OUTFILE' to a directory, that's to say, member outfile_target
    /// is set.
    virtual void customReleaseBuffer() { }

    void processMultiOutFileIfNeeded(size_t bytes);
public:
    IOutputFormat(const Block & header_, WriteBuffer & out_);

    Status prepare() override;
    void work() override;

    /// Flush output buffers if any.
    virtual void flush();

    void setAutoFlush() { auto_flush = true; }

    /// Value for rows_before_limit_at_least field.
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}

    /// Counter to calculate rows_before_limit_at_least in processors pipeline.
    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) { rows_before_limit_counter.swap(counter); }

    /// Notify about progress. Method could be called from different threads.
    /// Passed value are delta, that must be summarized.
    virtual void onProgress(const Progress & /*progress*/) {}

    /// Content-Type to set when sending HTTP response.
    virtual std::string getContentType() const { return "text/plain; charset=UTF-8"; }

    InputPort & getPort(PortKind kind) { return *std::next(inputs.begin(), kind); }

    /// Compatible to IBlockOutputStream interface

    void write(const Block & block);

    virtual void doWritePrefix() {}
    virtual void doWriteSuffix() { finalize(); }

    virtual bool expectMaterializedColumns() const { return true; }

    void setTotals(const Block & totals) { consumeTotals(Chunk(totals.getColumns(), totals.rows())); }
    void setExtremes(const Block & extremes) { consumeExtremes(Chunk(extremes.getColumns(), extremes.rows())); }

    size_t getResultRows() const { return result_rows; }
    size_t getResultBytes() const { return result_bytes; }

    static Chunk prepareTotals(Chunk chunk);

    void setOutFileTarget(OutfileTargetPtr outfile_target);

    virtual void closeFile();

    void setMPPQueryCoordinator(MPPQueryCoordinatorPtr coordinator_)
    {
        coordinator = std::move(coordinator_);
    }

    /// Reset the watch to a specific point in time
    /// If set to not running it will stop on the call (elapsed = now() - given start)
    void setStartTime(UInt64 start, bool is_running)
    {
        watch = Stopwatch(CLOCK_MONOTONIC, start, true);
        if (!is_running)
            watch.stop();
    }

private:
    /// Counters for consumed chunks. Are used for QueryLog.
    size_t result_rows = 0;
    size_t result_bytes = 0;

    bool prefix_written = false;
    MPPQueryCoordinatorPtr coordinator = nullptr;
};
}

