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

#ifndef REPO_TESTS_DATASET_FIXTURES_HPP
#define REPO_TESTS_DATASET_FIXTURES_HPP


#include <vector>
#include <boost/mpl/vector.hpp>
#include "sync/action-entry.hpp"
#include "storage/index.hpp"

namespace repo {
namespace tests {

class ActionSet
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

  typedef std::list<ndn::Name> CreatorContainer;
  CreatorContainer creators;

  typedef std::list<ActionEntry> ActionContainer;
  ActionContainer actions;

  typedef std::map<ndn::Name, uint64_t> SeqContainer;
  SeqContainer seqs;

  typedef std::map<ndn::Name, status> DataNameContainer;
  DataNameContainer dataNames;

  ActionEntry
  createAction(const ndn::Name& creatorName, const ndn::Name& dataName, const Action& action)
  {
    uint64_t seq = ++seqs[creatorName];
    //std::cout<<"creatorname = "<<creatorName<<" seqs[name] = "<<seqs[creatorName]<<std::endl;
    ActionEntry entry(creatorName, seq, action, dataName, 0);
    actions.push_back(entry);
    return entry;
  }
};

class GenerateAction : public ActionSet
{
public:
  static const std::string&
  getName()
  {
    static std::string name = "GenerateAction";
    return name;
  }

  GenerateAction()
  {
    this->creators.push_back("/repo/0");
    this->creators.push_back("/repo/1");
    this->creators.push_back("/repo/2");
    for (uint64_t i = 1; i < 10; i++) {
      for (CreatorContainer::iterator it = this->creators.begin(); it != this->creators.end(); it++) {
        Name data("/data");
        data.append(*it).appendNumber(i);
        createAction(*it, data, INSERTION);
      }
    }

    Name dataName("/data/test");
    for (uint64_t i = 1; i < 20; i++) {
      dataName.appendNumber(i);
      dataNames[dataName] = EXISTED;
    }
  }
};

typedef boost::mpl::vector<GenerateAction> ActionSets;

} // namespace tests
} // namespace repo

#endif // REPO_TESTS_DATASET_FIXTURES_HPP
