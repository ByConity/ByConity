#include <vector>
#include <IO/ReadBuffer.h>

namespace DB
{

class ConsecutiveReadBuffer: public ReadBuffer
{
public:
    explicit ConsecutiveReadBuffer(const std::vector<ReadBuffer*>& buffers_);

    size_t readBig(char* to, size_t n) override;

private:
    bool nextImpl() override;

    void syncToBuffer();

    size_t current_buffer;
    std::vector<ReadBuffer*> buffers;
};

}
