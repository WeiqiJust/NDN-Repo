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

#ifndef SYNC_INTEREST_TABLE_H
#define SYNC_INTEREST_TABLE_H

#include <ndn-cxx/util/scheduler.hpp>
#include <ndn-cxx/util/digest.hpp>
#include <string>
#include <vector>

#include "sync-interest-container.hpp"

namespace repo {

/**
 * @brief A table to keep unanswered Sync Interest
 */
class InterestTable
{
public:
  class Error : public std::runtime_error
  {
  public:
    explicit
    Error(const std::string& what)
      : std::runtime_error(what)
    {
    }
  };

  InterestTable(boost::asio::io_service& io,
                ndn::time::system_clock::Duration lifetime,
                ndn::time::system_clock::Duration cleanPeriod = ndn::time::seconds(4));

  /**
   * @brief Insert an interest
   *
   * if interest already exists, update the timestamp
   */
  bool
  insert(ndn::ConstBufferPtr digest, const Name& name, bool isKnown=false);

  /**
   * @brief Remove interest by digest (e.g., when it was satisfied)
   */
  void
  remove(ndn::ConstBufferPtr digest);

  /**
   * @brief pop a non-expired Interest from PIT
   */
  ConstPendingInterestPtr
  pop();

    /**
   * @brief get the first a non-expired Interest from PIT
   */
  ConstPendingInterestPtr
  begin();

  size_t
  size() const;

  void
  reset();

private:
  /**
   * @brief periodically called to clean expired Interest
   */
  void
  expireInterests();

private:
  ndn::Scheduler m_scheduler;
  ndn::time::system_clock::Duration m_entryLifetime;
  ndn::time::system_clock::Duration m_cleanPeriod;

  InterestContainer m_table;
};


} // repo

#endif // REPO_SYNC_SYNC_INTEREST_TABLE_H
