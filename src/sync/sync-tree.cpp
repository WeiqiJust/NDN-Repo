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
#include "sync-tree.hpp"

namespace repo {

void
SyncTree::init()
{
  m_treeStorage->fullEnumerate(bind(&SyncTree::readNodeFromDatabase, this, _1));
  m_root = calculateRootDigest();
}

void
SyncTree::readNodeFromDatabase(const TreeStorage::ItemMeta& item)
{
  std::map<Name, TreeEntry>::iterator it = m_nodes.find(item.creatorName);
  if (it != m_nodes.end()) {
    TreeEntry entry;
    entry.first = item.seq;
    entry.last = item.seq;
    entry.digest = computeDigest(item.creatorName, entry.last);
    m_nodes[item.creatorName] = entry;
  }
  else
    throw Error("The nodes in database is not unique");
}

ndn::ConstBufferPtr
SyncTree::update(const ActionEntry& action)
{
  Name creator = action.getCreatorName();
  std::map<Name, TreeEntry>::iterator it = m_nodes.find(creator);
  if (it == m_nodes.end()) {
    BOOST_ASSERT(action.getSeqNo() == 1);
    TreeEntry entry;
    entry.first = 0;
    entry.last = action.getSeqNo();
    entry.digest = computeDigest(creator, entry.last);
    m_nodes[creator] = entry;
    m_treeStorage->insert(creator, action.getSeqNo());
  }
  else {
    if (it->second.last < action.getSeqNo()) {
      it->second.last = action.getSeqNo();
      it->second.digest = computeDigest(creator, it->second.last);
      m_treeStorage->update(creator, action.getSeqNo());
    }
    else {
      // do nothing, this situation can only happen when fetching actions responses are out of order
    }
  }
  return calculateRootDigest();
}

ndn::ConstBufferPtr
SyncTree::computeDigest(const Name& name, const uint64_t seq)
{
  ndn::util::Sha256 digest;
  digest << name.toUri() << seq;
  return digest.computeDigest();
}

void
SyncTree::updateForSnapshot()
{
  for (std::map<Name, TreeEntry>::iterator it = m_nodes.begin(); it != m_nodes.end(); it++)
  {
    it->second.first = it->second.last;
  }
}

void
SyncTree::addNode(const Name& name)
{
  TreeEntry entry;
  entry.first = 0;
  entry.last = 0;
  entry.digest = computeDigest(name, entry.last);
  m_nodes[name] = entry;
  m_treeStorage->insert(name, 0);
}

ndn::ConstBufferPtr
SyncTree::calculateRootDigest()
{
  std::map<Name, TreeEntry>::iterator it = m_nodes.begin();
  ndn::util::Sha256 digest;
  while (it != m_nodes.end()) {
    digest.update(it->second.digest->buf(), it->second.digest->size());
    ++it;
  }
  m_root = digest.computeDigest();
  return m_root;
}

SyncTree::const_iter
SyncTree::lookup(const Name& creatorName) const
{
  return m_nodes.find(creatorName);
}

} // namespace repo
