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

#ifndef REPO_SYNC_SYNC_TREE_HPP
#define REPO_SYNC_SYNC_TREE_HPP

#include "common.hpp"
#include <ndn-cxx/util/digest.hpp>
#include "action-entry.hpp"
#include "tree-storage.hpp"
#include "tree-sqlite.hpp"

namespace repo {

struct TreeEntry
{
  uint64_t first;
  uint64_t last;
  ndn::ConstBufferPtr digest;
};

class SyncTree
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

  typedef std::map<Name, TreeEntry>::const_iterator const_iter;

  SyncTree(const std::string& dbPath)
    : m_treeStorage(make_shared<TreeSqlite>(dbPath))
  {
    init();
  }

  void
  init();

  void
  readNodeFromDatabase(const TreeStorage::ItemMeta& it);

  /**
   * @brief  update the digest tree using the received action
   * @return root digest
   */
  ndn::ConstBufferPtr
  update(const ActionEntry& action);

  ndn::ConstBufferPtr
  computeDigest(const Name& name, const uint64_t seq);

  void
  updateForSnapshot();

  /**
   * @brief  add a node in digest tree
   */
  void
  addNode(const Name& name);

  /**
   * @brief  calculate the current root digest, which represents the status
   * @return root digest
   */
  ndn::ConstBufferPtr
  calculateRootDigest();

  /**
   * @brief  to check whether there is a node with the specific creator name
   * @return iterator of the node if found, end() if not
   */
  const_iter
  lookup(const Name& creatorName) const;

  ndn::ConstBufferPtr
  getDigest() const
  {
    //std::cout<<"my digest is = "<<*m_root<<std::endl;
    return m_root;
  }

  const_iter
  begin() const
  {
    return m_nodes.begin();
  }

  const_iter
  end() const
  {
    return m_nodes.end();
  }

private:
  std::map<Name, TreeEntry> m_nodes;
  ndn::ConstBufferPtr m_root;
  shared_ptr<TreeStorage> m_treeStorage;
};

} // namespace repo

#endif // REPO_REPO_SYNC_TREE_HPP
