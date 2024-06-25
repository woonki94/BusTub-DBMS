#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "common/logger.h"
#include "fmt/format.h"

namespace bustub {

/*
 * Checks if an element in the set by iterating through element_set,
 * Ensure UID is not in the tombstone_set
 * */
template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  return std::any_of(element_set.begin(), element_set.end(),
                     [&elem, this](const Element &e) {
                       return e.value == elem && tombstone_set.find(e.uid) == tombstone_set.end();
                     });

  //throw NotImplementedException("ORSet<T>::Contains is not implemented");
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  element_set.insert({elem, uid});

  //throw NotImplementedException("ORSet<T>::Add is not implemented");
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  for (const auto &e : element_set) {
    if (e.value == elem) {
      tombstone_set.insert(e.uid);
    }
  }
  //throw NotImplementedException("ORSet<T>::Remove is not implemented");
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  element_set.insert(other.element_set.begin(), other.element_set.end());
  tombstone_set.insert(other.tombstone_set.begin(), other.tombstone_set.end());

  //throw NotImplementedException("ORSet<T>::Merge is not implemented");
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T> result;
  for (const auto &e : element_set) {
    if (tombstone_set.find(e.uid) == tombstone_set.end()) {
      result.push_back(e.value);
    }
  }
  return result;
  //throw NotImplementedException("ORSet<T>::Elements is not implemented");
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
