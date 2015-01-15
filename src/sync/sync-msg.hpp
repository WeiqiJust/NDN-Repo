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

#ifndef REPO_SYNC_SYNC_MSG_HPP
#define REPO_SYNC_SYNC_MSG_HPP

#include "common.hpp"
#include <ndn-cxx/util/digest.hpp>
#include "action-entry.hpp"
#include "sync-state.pb.h"
#include "storage/index.hpp"

namespace repo {

class Msg
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

public:
  Msg(SyncStateMsg_MsgType type);

  explicit
  Msg(const SyncStateMsg& msg);

  SyncStateMsg
  getMsg() const
  {
    return m_msg;
  }

  void
  setMsg(const SyncStateMsg& msg)
  {
    m_msg = msg;
  }

  /**
   * @brief  write multiple action names, including creator name and seqNo, into data
   */
  void
  writeActionNameToMsg(const ActionEntry& action);

  /**
   * @brief  write one action into data
   */
  void
  writeActionToMsg(const ActionEntry& action);

  void
  writeDataToSnapshot(const Name& name, const status & stat);

  void
  writeTreeToSnapshot(const Name& name, const uint64_t seq);

  void
  writeInfoToSnapshot(const Name& name, const uint64_t version);

  void
  readDataFromSnapshot(ndn::function< void (const Name &, const status &) > f);

  void
  readTreeFromSnapshot(ndn::function< void (const ActionEntry &) > f);

  std::pair<Name,uint64_t>
  readInfoFromSnapshot();

  /**
   * @brief  read multiple action names from the received data, and call the function to handle the action names
   */
  void
  readActionNameFromMsg(ndn::function< void (const Name &, const uint64_t &) > f, const Name& name);

  /**
   * @brief  read the entire action from received data, and call the function to handle the action
   */
  void
  readActionFromMsg(ndn::function< void (const ActionEntry &) > f);

private:
  SyncStateMsg m_msg;
  SyncStateMsg_MsgType m_type;
};

} // namespace repo

#endif // REPO_SYNC_REPO_SYNC_HPP
