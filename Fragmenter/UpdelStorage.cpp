/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <mutex>
#include <string>
#include <vector>
#include <limits>
#include <algorithm>
#include <boost/variant.hpp>
#include <boost/variant/get.hpp>

#include "Fragmenter/InsertOrderFragmenter.h"
#include "Shared/TypedDataAccessors.h"
#include "Shared/thread_count.h"
#include "DataMgr/DataMgr.h"
#include "Catalog/Catalog.h"

namespace Fragmenter_Namespace {

void InsertOrderFragmenter::updateColumn(const Catalog_Namespace::Catalog* catalog,
                                         const std::string& tabName,
                                         const std::string& colName,
                                         const int fragmentId,
                                         const std::vector<uint64_t>& fragOffsets,
                                         const std::vector<ScalarTargetValue>& rhsValues,
                                         const Data_Namespace::MemoryLevel memoryLevel,
                                         UpdelRoll& updelRoll) {
  const auto td = catalog->getMetadataForTable(tabName);
  CHECK(td);
  const auto cd = catalog->getMetadataForColumn(td->tableId, colName);
  CHECK(cd);
  td->fragmenter->updateColumn(catalog, td, cd, fragmentId, fragOffsets, rhsValues, memoryLevel, updelRoll);
}

void InsertOrderFragmenter::updateColumn(const Catalog_Namespace::Catalog* catalog,
                                         const TableDescriptor* td,
                                         const ColumnDescriptor* cd,
                                         const int fragmentId,
                                         const std::vector<uint64_t>& fragOffsets,
                                         const ScalarTargetValue& rhsValue,
                                         const Data_Namespace::MemoryLevel memoryLevel,
                                         UpdelRoll& updelRoll) {
  updateColumn(
      catalog, td, cd, fragmentId, fragOffsets, std::vector<ScalarTargetValue>(1, rhsValue), memoryLevel, updelRoll);
}

void InsertOrderFragmenter::updateColumn(const Catalog_Namespace::Catalog* catalog,
                                         const TableDescriptor* td,
                                         const ColumnDescriptor* cd,
                                         const int fragmentId,
                                         const std::vector<uint64_t>& fragOffsets,
                                         const std::vector<ScalarTargetValue>& rhsValues,
                                         const Data_Namespace::MemoryLevel memoryLevel,
                                         UpdelRoll& updelRoll) {
  updelRoll.catalog = catalog;
  updelRoll.tableDescriptor = td;
  updelRoll.insertOrderFragmenter = this;

  const auto nrow = fragOffsets.size();
  const auto nval = rhsValues.size();
  if (0 == nrow)
    return;
  CHECK(nrow == nval || 1 == nval);

  const auto fragment_it = std::find_if(fragmentInfoVec_.begin(),
                                        fragmentInfoVec_.end(),
                                        [=](const FragmentInfo& f) -> bool { return f.fragmentId == fragmentId; });
  CHECK(fragment_it != fragmentInfoVec_.end());
  const auto& fragment = *fragment_it;
  auto chunk_meta_it = fragment.getChunkMetadataMapPhysical().find(cd->columnId);
  CHECK(chunk_meta_it != fragment.getChunkMetadataMapPhysical().end());
  ChunkKey chunk_key{catalog->get_currentDB().dbId, td->tableId, cd->columnId, fragment.fragmentId};
  auto chunk = Chunk_NS::Chunk::getChunk(cd,
                                         &catalog->get_dataMgr(),
                                         chunk_key,
                                         Data_Namespace::CPU_LEVEL,
                                         0,
                                         chunk_meta_it->second.numBytes,
                                         chunk_meta_it->second.numElements);

  const auto ncore = (size_t)cpu_threads();

  std::vector<bool> null(ncore, false);
  std::vector<double> dmax(ncore, std::numeric_limits<double>::min());
  std::vector<double> dmin(ncore, std::numeric_limits<double>::max());
  std::vector<int64_t> lmax(ncore, std::numeric_limits<int64_t>::min());
  std::vector<int64_t> lmin(ncore, std::numeric_limits<int64_t>::max());

  // parallel update elements
  std::vector<std::future<void>> threads;
  std::exception_ptr failed_any_chunk;
  std::mutex mtx;
  auto wait_cleanup_threads = [&] {
    try {
      for (auto& t : threads)
        t.wait();
      for (auto& t : threads)
        t.get();
    } catch (...) {
      std::unique_lock<std::mutex> lck(mtx);
      failed_any_chunk = std::current_exception();
    }
    threads.clear();
  };

  const auto segsz = (nrow + ncore - 1) / ncore;
  const auto& lctype = cd->columnType;
  auto dbuf = chunk->get_buffer();
  auto d0 = dbuf->getMemoryPtr();
  dbuf->setUpdated();
  for (size_t rbegin = 0, c = 0; rbegin < nrow; ++c, rbegin += segsz) {
    threads.emplace_back(std::async(std::launch::async, [=, &null, &lmin, &lmax, &dmin, &dmax] {
      for (size_t r = rbegin; r < std::min(rbegin + segsz, nrow); r++) {
        const auto roffs = fragOffsets[r];
        auto dptr = d0 + roffs * get_uncompressed_element_size(lctype);
        const auto sv = &rhsValues[1 == nval ? 0 : r];
        if (const auto v = boost::get<int64_t>(sv)) {
          put_scalar<int64_t>(dptr, lctype, *v);
          if (lctype.is_integer())
            set_minmax<int64_t>(lmin[c], lmax[c], *v);
          else
            set_minmax<double>(dmin[c], dmax[c], *v);
        } else if (const auto v = boost::get<double>(sv)) {
          put_scalar<double>(dptr, lctype, *v);
          if (lctype.is_integer())
            set_minmax<int64_t>(lmin[c], lmax[c], *v);
          else
            set_minmax<double>(dmin[c], dmax[c], *v);
        } else if (const auto v = boost::get<float>(sv)) {
          put_scalar<float>(dptr, lctype, *v);
          if (lctype.is_integer())
            set_minmax<int64_t>(lmin[c], lmax[c], *v);
          else
            set_minmax<double>(dmin[c], dmax[c], *v);
        } else if (const auto v = boost::get<NullableString>(sv)) {
          const auto s = boost::get<std::string>(v);
          const auto sval = s ? *s : std::string("");
          if (lctype.is_string()) {
            CHECK(kENCODING_DICT == lctype.get_compression());
            auto dictDesc = catalog->getMetadataForDict(cd->columnType.get_comp_param());
            CHECK(dictDesc);
            auto stringDict = dictDesc->stringDict.get();
            CHECK(stringDict);
            auto sidx = stringDict->getOrAdd(sval);
            put_scalar<int32_t>(dptr, lctype, sidx);
            set_minmax<int64_t>(lmin[c], lmax[c], sidx);
          } else if (sval.size() > 0) {
            auto dval = std::atof(sval.data());
            if (lctype.is_boolean())
              dval = sval == "t" || sval == "true" || sval == "T" || sval == "True";
            if (lctype.is_fp() || lctype.is_decimal()) {
              put_scalar<double>(dptr, lctype, dval);
              set_minmax<double>(dmin[c], dmax[c], dval);
            } else {
              put_scalar<int64_t>(dptr, lctype, dval);
              set_minmax<int64_t>(lmin[c], lmax[c], dval);
            }
          } else {
            put_null(dptr, lctype, cd->columnName);
            null[c] = true;
          }
        } else
          CHECK(false);
      }
    }));
    if (threads.size() >= (size_t)cpu_threads())
      wait_cleanup_threads();
    if (failed_any_chunk)
      break;
  }
  wait_cleanup_threads();
  if (failed_any_chunk)
    std::rethrow_exception(failed_any_chunk);

  bool lc_null{false};
  double lc_dmax{std::numeric_limits<double>::min()};
  double lc_dmin{std::numeric_limits<double>::max()};
  int64_t lc_lmax{std::numeric_limits<int64_t>::min()};
  int64_t lc_lmin{std::numeric_limits<int64_t>::max()};
  for (size_t c = 0; c < ncore; ++c) {
    lc_null |= null[c];
    lc_dmax = std::max<double>(lc_dmax, dmax[c]);
    lc_dmin = std::min<double>(lc_dmin, dmin[c]);
    lc_lmax = std::max<int64_t>(lc_lmax, lmax[c]);
    lc_lmin = std::min<int64_t>(lc_lmin, lmin[c]);
  }
  updateColumnMetadata(cd, fragment, chunk, lc_null, lc_dmax, lc_dmin, lc_lmax, lc_lmin, updelRoll);
}

void InsertOrderFragmenter::updateColumnMetadata(const ColumnDescriptor* cd,
                                                 const FragmentInfo& fragment,
                                                 std::shared_ptr<Chunk_NS::Chunk> chunk,
                                                 const bool null,
                                                 const double dmax,
                                                 const double dmin,
                                                 const int64_t lmax,
                                                 const int64_t lmin,
                                                 UpdelRoll& updelRoll) {
  std::lock_guard<std::mutex> lck(updelRoll.mutex);
  if (0 == updelRoll.chunkMetadata.count(fragment.fragmentId))
    updelRoll.chunkMetadata[fragment.fragmentId] = fragment.getChunkMetadataMapPhysical();
  if (0 == updelRoll.numTuples.count(fragment.fragmentId))
    updelRoll.numTuples[fragment.fragmentId] = fragment.shadowNumTuples;
  auto& chunkMetadata = updelRoll.chunkMetadata[fragment.fragmentId];

  auto buffer = chunk->get_buffer();
  const auto& lctype = cd->columnType;
  if (lctype.is_integer()) {
    buffer->encoder->updateStats(lmax, null);
    buffer->encoder->updateStats(lmin, null);
    chunkMetadata[cd->columnId].fillChunkStats<int64_t>(lmin, lmax, null);
  } else if (lctype.is_fp()) {
    buffer->encoder->updateStats(dmax, null);
    buffer->encoder->updateStats(dmin, null);
    chunkMetadata[cd->columnId].fillChunkStats<double>(lmin, lmax, null);
  } else if (lctype.is_decimal()) {
    buffer->encoder->updateStats((int64_t)(dmax * pow(10, lctype.get_scale())), null);
    buffer->encoder->updateStats((int64_t)(dmin * pow(10, lctype.get_scale())), null);
    chunkMetadata[cd->columnId].fillChunkStats<int64_t>(lmin, lmax, null);
  } else if (!lctype.is_array() && !(lctype.is_string() && kENCODING_DICT != lctype.get_compression())) {
    buffer->encoder->updateStats(lmax, null);
    buffer->encoder->updateStats(lmin, null);
    chunkMetadata[cd->columnId].fillChunkStats<int64_t>(lmin, lmax, null);
  }

  if (updelRoll.dirty_chunks.count(chunk.get()) == 0)
    updelRoll.dirty_chunks.emplace(chunk.get(), chunk);

  fragment.invalidateChunkMetadataMap();
}

void InsertOrderFragmenter::updateMetadata(const Catalog_Namespace::Catalog* catalog,
                                           const TableDescriptor* td,
                                           UpdelRoll& updelRoll) {
  mapd_unique_lock<mapd_shared_mutex> writeLock(fragmentInfoMutex_);
  for (auto& fragmentInfo : fragmentInfoVec_)
    if (updelRoll.chunkMetadata.count(fragmentInfo.fragmentId)) {
      const auto& chunkMetadata = updelRoll.chunkMetadata[fragmentInfo.fragmentId];
      fragmentInfo.shadowChunkMetadataMap = chunkMetadata;
      fragmentInfo.setChunkMetadataMap(chunkMetadata);
      fragmentInfo.shadowNumTuples = updelRoll.numTuples[fragmentInfo.fragmentId];
      fragmentInfo.setPhysicalNumTuples(fragmentInfo.shadowNumTuples);
      // TODO(ppan): When fragment-level compaction is enable, the following code should suffice.
      // When not (ie. existing code), we'll revert to update InsertOrderFragmenter::varLenColInfo_
      /*
      for (const auto cit : chunkMetadata) {
        const auto& cd = *catalog->getMetadataForColumn(td->tableId, cit.first);
        if (cd.columnType.get_size() < 0)
          fragmentInfo.varLenColInfox[cd.columnId] = cit.second.numBytes;
      }
      */
    }
}

}  // namespace Fragmenter_Namespace

void UpdelRoll::commitUpdate() {
  catalog->get_dataMgr().checkpoint(catalog->get_currentDB().dbId, tableDescriptor->tableId);
  insertOrderFragmenter->updateMetadata(catalog, tableDescriptor, *this);
  dirty_chunks.clear();
}

void UpdelRoll::cancelUpdate() {
  for (auto dit : dirty_chunks) {
    catalog->get_dataMgr().free(dit.first->get_buffer());
    dit.first->set_buffer(nullptr);
  }
}
