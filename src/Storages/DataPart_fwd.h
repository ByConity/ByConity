#pragma once

#include <memory>
#include <vector>

namespace DB
{
	class FileDataPart;

	using FileDataPartsCNCHPtr = std::shared_ptr<const FileDataPart>;
	using FileDataPartsCNCHVector = std::vector<FileDataPartsCNCHPtr>;
}

