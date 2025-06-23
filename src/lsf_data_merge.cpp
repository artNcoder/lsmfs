#include "lsf_data_merge.h"

using ROCKSDB_NAMESPACE::AssociativeMergeOperator;
using ROCKSDB_NAMESPACE::MergeOperator;
using ROCKSDB_NAMESPACE::Slice;

static bool LsfMerge(const Slice &key,
                     const Slice *left_operand,
                     const Slice &right_operand,
                     std::string *new_value,
                     rocksdb::Logger *logger)
{
    LOG_DEBUG("this is LsfMerge");
    if (left_operand == NULL || left_operand->size() == 0)
    {
        LOG_INFO("LsfMerge left_operand = NULL");
        new_value->resize(right_operand.size());
        memcpy(new_value->data(), right_operand.data(), right_operand.size());
        return true;
    }
    const struct block_head *left_blk_head = (const struct block_head *)left_operand->data();
    const char *left_data_buf = ((const char *)left_blk_head) + sizeof(struct block_head);

    const struct block_head *right_blk_head = (const struct block_head *)right_operand.data();
    char *right_data_buf = ((char *)right_blk_head) + sizeof(struct block_head);
    const struct block_key *blk_key = (const struct block_key *)key.data();
        LOG_INFOF("Merge detect: "
                   "left.data_bmp=%u, right.data_bmp=%u",
                   (unsigned)left_blk_head->data_bmp,
                   (unsigned)right_blk_head->data_bmp);
    assert(left_blk_head->data_bmp != LSF_FULL_BLOCK_BMP && right_blk_head->data_bmp != LSF_FULL_BLOCK_BMP);
    LOG_DEBUGF("Merge extent:%s with left:(%hu,%ld) + right:(%hu,%ld) bytes", blk_key->to_string(),
               left_blk_head->merge_off, left_operand->size() - sizeof(struct block_head),
               right_blk_head->merge_off, right_operand.size() - sizeof(struct block_head));

    size_t blk_begin = std::min(left_blk_head->merge_off, right_blk_head->merge_off);
    size_t blk_end = std::max(left_blk_head->merge_off + left_operand->size() - sizeof(struct block_head),
                              right_blk_head->merge_off + right_operand.size() - sizeof(struct block_head));

    new_value->resize(blk_end - blk_begin + sizeof(struct block_head));
    struct block_head *new_blk_head = (struct block_head *)new_value->data();
    char *new_data_buf = ((char *)new_blk_head) + sizeof(struct block_head);
    memcpy(new_data_buf + (left_blk_head->merge_off - blk_begin), left_data_buf, left_operand->size() - sizeof(struct block_head));
    memcpy(new_data_buf + (right_blk_head->merge_off - blk_begin), right_data_buf, right_operand.size() - sizeof(struct block_head));
    new_blk_head->merge_off = (int16_t)blk_begin;
    // assert(new_ext_head->data_bmp != PFS_FULL_EXTENT_BMP);
    LOG_DEBUGF("Merge done, new off:%ld", new_blk_head->merge_off);
    return true;

    //	return true;
}

bool FuseMergeOperator::PartialMerge(const Slice &key,
                                     const Slice &left_operand,
                                     const Slice &right_operand,
                                     std::string *new_value,
                                     rocksdb::Logger *logger) const
{
    if (left_operand.size() == 0) {
        return LsfMerge(key, nullptr, right_operand, new_value, logger);
    } else {
        return LsfMerge(key, &left_operand, right_operand, new_value, logger);
    }
    //return LsfMerge(key, &left_operand, right_operand, new_value, logger);
}

bool FuseMergeOperator::FullMergeV2(const MergeOperationInput &merge_in,
                                    MergeOperationOutput *merge_out) const
{
    LOG_DEBUG("this is FullMergeV2");
    const struct block_key *blk_key = (const struct block_key *)merge_in.key.data();
    LOG_DEBUGF("FullMergeV2 block:%s with %d operand", blk_key->to_string(), merge_in.operand_list.size());

    // merge_out->new_value：合并后要写回 RocksDB 的完整 value，[ block_head | data … ]。
    merge_out->new_value.resize(BLOCK_SIZE + sizeof(struct block_head));
    char *new_buf = merge_out->new_value.data();
    char *new_data_buf = new_buf + sizeof(struct block_head);

    // merge_in.existing_value 如果之前已经有一个「合并结果」，会传进来；首轮可能为空
    if (merge_in.existing_value != NULL)
    {
        LOG_DEBUGF("FullMergeV2 with existing_value size %ld", merge_in.existing_value->size());
        const struct block_head *existing_blk_head = (const struct block_head *)merge_in.existing_value->data();
        const char *existing_data_buf = merge_in.existing_value->data() + sizeof(struct block_head);
        // assert(existing_ext_head->data_bmp == PFS_FULL_EXTENT_BMP);//suppose base data are full filled
        memcpy(new_data_buf, existing_data_buf, merge_in.existing_value->size() - sizeof(struct block_head));
    }

    int i = 0;
    // merge_in.operand_list：一个数组，保存了多次针对同一个 block 的写入片段，每个元素都是一个 Slice，内部格式是 [ block_head | data … ]
    for (const Slice &value : merge_in.operand_list)
    {
        if (value.size() == 0)
        {
            LOG_DEBUGF("skip operand[%d] whose length is 0", i++);
            continue;
        }
        else
        {
            
            const char *buf = value.data();
            const struct block_head *blk_head = (const struct block_head *)buf;
            const char *data_buf = buf + sizeof(struct block_head);
            size_t len = value.size() - sizeof(struct block_head);
           // LOG_DEBUGF("Operand[%d] length is %d, merge_off=%d, len=%zu", i, value.size(),blk_head->merge_off, len);
            memcpy(new_data_buf + blk_head->merge_off, data_buf, value.size() - sizeof(struct block_head));
            i++;
        }
    }

    return true;
    //            return true;
}
