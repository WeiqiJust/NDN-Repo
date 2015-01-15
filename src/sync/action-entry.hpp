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

#ifndef REPO_SYNC_ACTION_ENTRY_HPP
#define REPO_SYNC_ACTION_ENTRY_HPP

#include "common.hpp"
#include "action-entry.hpp"
namespace repo {

enum Action
{
  INSERTION,
  DELETION,
  OTHERS
};

class ActionEntry
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
   
  /**
   * @brief used when local handle generate an action
   */
  ActionEntry(const Name& creatorName, const Name& dataName, const Action& action);

  /**
   * @brief used to construct the entry from received action name
   */
  ActionEntry(const Name& creatorName, const uint64_t seqNo);

  /**
   * @brief used to construct an entry from received action
   */
  ActionEntry(const Name& creatorName, const uint64_t seqNo, const Action& action,
              const Name& dataName, const uint64_t version);

  Name
  getName() const
  {
    return m_name;
  }

  Name
  getDataName() const
  {
    return m_dataName;
  }

  Name
  getCreatorName() const
  {
    return m_creator;
  }

  Action
  getAction() const
  {
    return m_action;
  }

  uint64_t
  getSeqNo() const
  {
    return m_seqNo;
  }

  uint64_t
  getVersion() const
  {
    return m_version;
  }

  void
  setVersion(const uint64_t version)
  {
    m_version = version;
  }

  void
  setSeqNo(const uint64_t seqNo)
  {
    m_seqNo = seqNo;
  }

  void
  constructName();

  bool
  operator==(const ActionEntry& action) const
  {
    return m_dataName == action.getDataName() &&
           m_name == action.getName() &&
           m_version == action.getVersion();
  }

  bool
  operator!=(const ActionEntry& action) const
  {
    return !(*this == action);
  }

  bool
  operator < (const ActionEntry& action) const
  {
    return m_seqNo < action.getSeqNo();
  }


private:
  Name m_name;
  Name m_creator;
  Name m_dataName;
  Action m_action;
  uint64_t m_seqNo;    // seqNo will be settled by action detector
  uint64_t m_version;  // version will be settled by action detector
};

} // namespace repo

#endif // REPO_SYNC_ACTION_ENTRY_HPP
