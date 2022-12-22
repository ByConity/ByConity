#include <DataStreams/CountingBlockOutputStream.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event InsertedRows;
    extern const Event InsertedBytes;
}


namespace DB
{

void CountingBlockOutputStream::write(const Block & block)
{
    stopwatch.start();

    stream->write(block);

    Progress local_progress(block.rows(), block.bytes(), 0, stopwatch.elapsedMilliseconds());
    progress.incrementPiecewiseAtomically(local_progress);

    ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.read_rows);
    ProfileEvents::increment(ProfileEvents::InsertedBytes, local_progress.read_bytes);

    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);
}

void CountingBlockOutputStream::writePrefix()
{
    stopwatch.start();

    stream->writePrefix();

    Progress local_progress(0, 0, 0, stopwatch.elapsedMilliseconds());
    progress.incrementPiecewiseAtomically(local_progress);

    /// Update the write duration to progress
    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);
}

void CountingBlockOutputStream::writeSuffix()
{
    stopwatch.start();

    stream->writeSuffix();

    Progress local_progress(0, 0, 0, stopwatch.elapsedMilliseconds());
    progress.incrementPiecewiseAtomically(local_progress);

    /// Update the write duration to progress
    if (process_elem)
        process_elem->updateProgressOut(local_progress);

    if (progress_callback)
        progress_callback(local_progress);
}


}
