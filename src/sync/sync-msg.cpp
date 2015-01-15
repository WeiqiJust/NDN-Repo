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


#include "sync-msg.hpp"

namespace repo {

Msg::Msg(SyncStateMsg_MsgType type)
  : m_type(type)
{
  m_msg.set_type(m_type);
}

Msg::Msg(const SyncStateMsg& msg)
  : m_msg(msg)
  , m_type(m_msg.type())
{
}

// The process can be simplified later by only write actionName into the message,
// action name /creatorName/seqNo
void
Msg::writeActionNameToMsg(const ActionEntry& action)
{
  BOOST_ASSERT(m_type == SyncStateMsg::ACTION);
  SyncState *oss = m_msg.add_ss();
  oss->set_name(action.getCreatorName().toUri());
  oss->set_seq(action.getSeqNo());
}

// need to be further check to make sure it is consistent with sync-state.pb.h
void
Msg::writeActionToMsg(const ActionEntry& action)
{
  BOOST_ASSERT(m_type == SyncStateMsg::ACTION);
  SyncState *oss = m_msg.add_ss();
  oss->set_name(action.getCreatorName().toUri());
  oss->set_seq(action.getSeqNo());
  if (action.getAction() == DELETION)
  {
    oss->set_type(SyncState::DELETE);
  }
  else
  {
    oss->set_type(SyncState::INSERT);
  }
  oss->set_dataname(action.getDataName().toUri());
  oss->set_version(action.getVersion());
}

void
Msg::writeDataToSnapshot(const Name& name, const status & stat)
{
  BOOST_ASSERT(m_type == SyncStateMsg::SNAPSHOT);
  SyncData *oss = m_msg.add_data();
  oss->set_dataname(name.toUri());
  switch (stat) {
    case EXISTED:
      oss->set_stat(SyncData::EXISTED);
      break;
    case DELETED:
      oss->set_stat(SyncData::DELETED);
      break;
    case INSERTED:
      oss->set_stat(SyncData::INSERTED);
      break;
    default:
      throw Error("Data status is not correct");
      break;
    }

}

void
Msg::writeTreeToSnapshot(const Name& name, const uint64_t seq)
{
  BOOST_ASSERT(m_type == SyncStateMsg::SNAPSHOT);
  SyncTreeNode *oss = m_msg.add_node();
  oss->set_creatorname(name.toUri());
  oss->set_seq(seq);
}

void
Msg::writeInfoToSnapshot(const Name& name, const uint64_t version)
{
  BOOST_ASSERT(m_type == SyncStateMsg::SNAPSHOT);
  m_msg.set_name(name.toUri());
  m_msg.set_version(version);
}

void
Msg::readDataFromSnapshot(ndn::function< void (const Name &, const status&) > f)
{
  BOOST_ASSERT(m_msg.type() == SyncStateMsg::SNAPSHOT);
  int n = m_msg.data_size();
  for (int i = 0; i < n; i++)
  {
    const SyncData& data = m_msg.data(i);
    if (!data.has_dataname()) {
      throw Error("Cannot read creator name from the received snapshot");
    }
    if (!data.has_stat()) {
      throw Error("Cannot read status from the received snapshot");
    }
    status stat;
    if (data.stat() == SyncData::EXISTED)
    {
      stat = EXISTED;
    }
    else if (data.stat() == SyncData::DELETED)
    {
      stat = DELETED;
    }
    else if (data.stat() == SyncData::INSERTED)
    {
      stat = INSERTED;
    }
    else
    {
      throw Error("Data status is not correct in the received snapshot");
    }
    f(Name(data.dataname()), stat);
  }
}

void
Msg::readTreeFromSnapshot(ndn::function< void (const ActionEntry &) > f)
{
  BOOST_ASSERT(m_msg.type() == SyncStateMsg::SNAPSHOT);
  int n = m_msg.node_size();
  for (int i = 0; i < n; i++)
  {
    const SyncTreeNode &node = m_msg.node(i);
    ActionEntry entry(Name(node.creatorname()), node.seq());
    f(entry);
  }
}

std::pair<Name,uint64_t>
Msg::readInfoFromSnapshot()
{
  BOOST_ASSERT(m_msg.type() == SyncStateMsg::SNAPSHOT);
  if (!m_msg.has_name()) {
      throw Error("Cannot read creator name from info int the received snapshot");
  }

  if (!m_msg.has_version()) {
    throw Error("Cannot read sequence number from info in the received snapshot");
  }
  return std::make_pair(Name(m_msg.name()), boost::lexical_cast<uint64_t>(m_msg.version()));
}

void
Msg::readActionNameFromMsg(ndn::function< void (const Name &, const uint64_t &) > f, const Name& name)
{
  BOOST_ASSERT(m_msg.type() == SyncStateMsg::ACTION);
  int n = m_msg.ss_size();
  for (int i = 0; i < n; i++)
  {
    const SyncState &ss = m_msg.ss(i);
    // if the action is produced by the caller (repo itself), actions should not be synchronized
    if (Name(ss.name()) == name)
      continue;
    if (!ss.has_name()) {
      throw Error("Cannot read creator name from the received action name");
    }

    if (!ss.has_seq()) {
      throw Error("Cannot read sequence number from the received action name");
    }

    uint64_t seq = boost::lexical_cast<uint64_t>(ss.seq());
    f(Name(ss.name()), seq);
  }
}

// read from fetch data, one data only has one aciton
void
Msg::readActionFromMsg(ndn::function< void (const ActionEntry & ) > f)
{
  BOOST_ASSERT(m_msg.type() == SyncStateMsg::ACTION);
  BOOST_ASSERT(m_msg.ss_size() == 1);
  const SyncState &ss = m_msg.ss(0);

  if (!ss.has_name()) {
    throw Error("Cannot read creator name from the received action");
  }

  if (!ss.has_seq()) {
    throw Error("Cannot read sequence number from the received action");
  }

  if (!ss.has_type()) {
    throw Error("Cannot read such action type from the received action!");
  }

  if (!ss.has_dataname()) {
    throw Error("Cannot read data name from the received action!");
  }

  if (!ss.has_version()) {
    throw Error("Cannot read version number from the received action!");
  }

  uint64_t seq = boost::lexical_cast<uint64_t>(ss.seq());
  Action action;
  if (ss.type() == SyncState::INSERT)
  {
    action = INSERTION;
  }
  else if (ss.type() == SyncState::DELETE)
  {
    action = DELETION;
  }
  else
  {
    throw Error("Cannot support such action type!");
  }
  uint64_t version = boost::lexical_cast<uint64_t>(ss.version());
  ActionEntry entry(Name(ss.name()), seq, action, Name(ss.dataname()), version);
  // std::cout<<"readActionFromMesg name = "<<entry.getName()<<std::endl;
  f(entry);
}

} // namespace repo
