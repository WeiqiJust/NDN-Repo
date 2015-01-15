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

#include "sync/sync-tree.hpp"
#include "sync/action-entry.hpp"
#include "../action-fixture.hpp"
#include <boost/test/unit_test.hpp>
#include <iostream>

namespace repo {
namespace tests {

BOOST_AUTO_TEST_SUITE(SyncTree)

template<class ActionSet>
class TreeFixture : public ActionSet
{
protected:
  TreeFixture()
    : m_syncTree("unittestdb_synctree")
  {
  }

  void
  insert(const ActionEntry& entry)
  {
    m_syncTree.update(entry);
  }

  uint64_t
  getSeq(const Name& name)
  {
    repo::SyncTree::const_iter it = m_syncTree.lookup(name);
    return it->second.last;
  }

  void
  addNode(const Name& name)
  {
    m_syncTree.addNode(name);
  }

  uint64_t
  size()
  {
    uint64_t count = 0;
    for (repo::SyncTree::const_iter it = m_syncTree.begin(); it != m_syncTree.end(); it++) {
      count++;
    }
    return count;
  }

protected:
  repo::SyncTree m_syncTree;
};


typedef boost::mpl::vector<GenerateAction> ActionSets;


BOOST_FIXTURE_TEST_CASE_TEMPLATE(Bulk, T, ActionSets, TreeFixture<T>)
{
  BOOST_TEST_MESSAGE(T::getName());
  for (typename T::ActionContainer::iterator i = this->actions.begin();
       i != this->actions.end(); ++i)
  {
    //std::cout<<"insert name"<<i->getName()<<std::endl;
    this->insert(*i);
  }
  for (typename T::CreatorContainer::iterator i = this->creators.begin();
       i != this->creators.end(); ++i)
  {
    BOOST_CHECK_EQUAL(this->getSeq(*i), this->seqs[*i]);
  }

  Name name("/addNode");
  this->addNode(name);
  BOOST_CHECK_EQUAL(this->size(), this->seqs.size() + 1);
  BOOST_CHECK_EQUAL(this->getSeq(name), 0);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace tests
} // namespace repo
