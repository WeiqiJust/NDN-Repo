/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014, Regents of the University of California.
 *
 * This file is part of NDN repo-ng (Next generation of NDN repository).
 * See AUTHORS.md for complete list of repo-ng authors and contributors.
 *
 * repo-ng is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * repo-ng is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * repo-ng, e.g., in COPYING.md file. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef REPO_SYNC_SYNC_INTEREST_CONTAINER_H
#define REPO_SYNC_SYNC_INTEREST_CONTAINER_H

#include <ndn-cxx/util/time.hpp>

#include <ndn-cxx/util/digest.hpp>
#include "common.hpp"
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/tag.hpp>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>

namespace mi = boost::multi_index;

namespace repo {

namespace mi = boost::multi_index;

class PendingInterest
{
public:
  PendingInterest(ndn::ConstBufferPtr digest, const Name& name, bool isUnknown=false)
    : m_digest(digest)
    , m_name(name)
    , m_time(ndn::time::system_clock::now())
    , m_isUnknown(isUnknown)
  {
  }

  ndn::ConstBufferPtr   m_digest;
  Name                  m_name;
  ndn::time::system_clock::TimePoint m_time;
  bool                  m_isUnknown;
};

typedef boost::shared_ptr<PendingInterest> PendingInterestPtr;
typedef boost::shared_ptr<const PendingInterest> ConstPendingInterestPtr;

struct timed
{
};

struct hashed
{
};

struct DigestPtrHash
{
  std::size_t
  operator()(ndn::ConstBufferPtr digest) const
  {
    BOOST_ASSERT(digest->size() > sizeof(std::size_t));

    return *reinterpret_cast<const std::size_t*>(digest->buf());
  }
};

struct DigestPtrEqual
{
  bool
  operator()(ndn::ConstBufferPtr digest1, ndn::ConstBufferPtr digest2) const
  {
    return *digest1 == *digest2;
  }
};

/**
 * @brief Container for pending interests (application PIT)
 */
struct InterestContainer : public mi::multi_index_container<
  PendingInterestPtr,
  mi::indexed_by<
    mi::hashed_unique<
      mi::tag<hashed>,
      mi::member<PendingInterest, ndn::ConstBufferPtr, &PendingInterest::m_digest>,
      DigestPtrHash,
      DigestPtrEqual
      >,

    mi::ordered_non_unique<
      mi::tag<timed>,
      mi::member<PendingInterest, ndn::time::system_clock::TimePoint, &PendingInterest::m_time>
      >
    >
  >
{
};

} // namespace repo

#endif // REPO_SYNC_SYNC_INTEREST_CONTAINER_H
