//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  //lock mutex
  //automatically unlock when out of scope
  std::lock_guard<std::mutex> guard(latch_);
  //return false when no frames can be evicted
  if(curr_size_ == 0)
    return false;

  frame_id_t victim_frame= -1;
  size_t max_k_distance = 0;
  size_t earliest_time = SIZE_MAX;

  for(auto &entry : node_store_) {
    auto &node = entry.second;

    //Only frames that are marked as 'evictable' are candidates for eviction.
    if (!node.IsEvictable()) continue;

    size_t k_distance = node.history_.size() < k_ ? SIZE_MAX : current_timestamp_ - node. history_.front();
    size_t timestamp = node.history_.empty() ? 0 : node.history_.back();

    //evict frame with earliest timestamp
    if (k_distance > max_k_distance || (k_distance == max_k_distance && timestamp < earliest_time)) {
      max_k_distance = k_distance;
      earliest_time = timestamp;
      victim_frame = entry.first;
    }
  }

  if(victim_frame == -1)
    return false;

  /* Successful eviction of a frame should decrement the size of replacer
   * and remove the frame's access history.
   */
  *frame_id = victim_frame;
  node_store_.erase(victim_frame);
  evictable_set_.erase(victim_frame);
  curr_size_--;
  return true;

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);

  // If frame id is invalid (ie. larger than replacer_size_), throw an exception.
  if (static_cast<size_t>(frame_id) >= replacer_size_) throw bustub::Exception("larger than replacer_size_");

  current_timestamp_++;
  auto &node = node_store_[frame_id];
  //node.AddTimestamp(current_timestamp_);


    node.history_.push_back(current_timestamp_);
    if(node.history_.size() > k_)
      node.history_.pop_front();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);

  auto &node = node_store_[frame_id];
  if(node.IsEvictable() && !set_evictable){
    evictable_set_.erase(frame_id);
    curr_size_--;
  }
  else if(!node.IsEvictable() && set_evictable){
    evictable_set_.insert(frame_id);
    curr_size_++;
  }

  node.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if(static_cast<size_t>(frame_id) >= replacer_size_)
    throw bustub::Exception("Larger than replacer_size_");

  auto it = node_store_.find(frame_id);
  if(it != node_store_.end() && it->second.IsEvictable()){
    node_store_.erase(it);
    evictable_set_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
