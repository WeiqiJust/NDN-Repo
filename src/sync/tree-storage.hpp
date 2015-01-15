/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014,  Regents of the University of California.
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
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * repo-ng, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef REPO_SYNC_TREE_STORAGE_HPP
#define REPO_SYNC_TREE_STORAGE_HPP
#include <string>
#include <iostream>
#include <stdlib.h>
#include "../common.hpp"

namespace repo {

/**
  * @brief Storage is a virtual abstract class which will be called by TreeSqlite
  */
class TreeStorage : noncopyable
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
  class ItemMeta
  {
  public:
    uint64_t seq;
    Name creatorName;
  };

public :

  virtual
  ~TreeStorage()
  {
  };

  /**
   *  @brief  put sync tree node into database
   */
  virtual void
  insert(const Name& creator, const uint64_t seq) = 0;

  /**
   *  @brief  update the seq number for the given creator name
   */
  virtual void
  update(const Name& creator, const uint64_t seq) = 0;

  /**
   *  @brief  remove tree node from database
   */
  virtual bool
  erase(const Name& creator) = 0;

  /**
   *  @brief  get sequence number of certain creator
   */
  virtual uint64_t
  read(const Name& creator) = 0;

  /**
   *  @brief  return the size of database, the number of tree nodes
   */
  virtual uint64_t
  size() = 0;

  /**
   *  @brief enumerate each entry in database and call the function
   *         certain to reubuild sync tree from database
   */
  virtual void
  fullEnumerate(const ndn::function<void(const TreeStorage::ItemMeta)>& f) = 0;

};

} // namespace repo

#endif // REPO_SYNC_TREE_STORAGE_HPP
