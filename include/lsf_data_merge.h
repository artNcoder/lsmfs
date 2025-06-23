#ifndef LSF_DATA_MERGE_H
#define LSF_DATA_MERGE_H

#include <rocksdb/merge_operator.h>
#include <nlohmann/json.hpp>
#include <string>
#include "lsf_i.h"

using ROCKSDB_NAMESPACE::MergeOperator;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::AssociativeMergeOperator;

class FuseMergeOperator : public MergeOperator
{
public:
	virtual bool PartialMerge(const Slice& key,
		const Slice& left_operand,
		const Slice& right_operand,
		std::string* new_value,
		rocksdb::Logger* logger) const override ;

	//virtual bool PartialMergeMulti(const Slice& key,
	//	const std::deque<Slice>& operand_list,
	//	std::string* new_value,
	//	Logger* logger) const;
	virtual bool FullMergeV2(const MergeOperationInput& merge_in,
		MergeOperationOutput* merge_out) const override ;

	virtual const char* Name() const override {
		return "FuseMergeOperator";
	}
};
#endif 
