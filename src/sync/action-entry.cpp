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

#include "action-entry.hpp"

namespace repo {

ActionEntry::ActionEntry(const Name& creatorName, const Name& dataName, const Action& action)
  : m_creator(creatorName)
  , m_dataName(dataName)
  , m_action(action)
  , m_seqNo(0)
  , m_version(0)
{
  constructName();
}

ActionEntry::ActionEntry(const Name& creatorName, const uint64_t seqNo)
  : m_creator(creatorName)
  , m_seqNo(seqNo)
{
  constructName();
}

ActionEntry::ActionEntry(const Name& creatorName, const uint64_t seqNo, const Action& action,
                         const Name& dataName, const uint64_t version)
  : m_creator(creatorName)
  , m_dataName(dataName)
  , m_action(action)
  , m_seqNo(seqNo)
  , m_version(version)
{
  constructName();
}

void
ActionEntry::constructName()
{
  Name name = m_creator;
  m_name = name.appendNumber(m_seqNo);
}

} // namespace repo
