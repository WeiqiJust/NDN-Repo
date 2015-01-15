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

#include "sync/sync-msg.hpp"
#include "sync/action-entry.hpp"
#include "../action-fixture.hpp"
#include <boost/test/unit_test.hpp>
#include <boost/exception/all.hpp>
#include <iostream>
#include "storage/index.hpp"

namespace repo {
namespace tests {

BOOST_AUTO_TEST_SUITE(SyncMsg)

struct SyncStateMsgDecodingFailure : virtual boost::exception, virtual std::exception { };

struct DigestCalculationError : virtual boost::exception, virtual std::exception{ };

static bool
compareSnapshot(ActionEntry entry1, ActionEntry entry2)
{
  return entry1.getName() == entry2.getName();
}

template<class ActionSet>
class MsgFixture : public ActionSet
{
public:
  MsgFixture()
  {
  }

  void
  readAction(const ActionEntry & action)
  {
    typename ActionSet::ActionContainer::iterator it =
                               std::find(this->actions.begin(), this->actions.end(), action);
    if (it == this->actions.end())
      BOOST_CHECK_EQUAL(true, false);
  }

  void
  readActionName(const Name& name, const uint64_t & seq)
  {
    ActionEntry action(name, seq);
    typename ActionSet::ActionContainer::iterator it =
                               std::find_if(this->actions.begin(), this->actions.end(),
                               bind(&compareSnapshot, _1, action));
    if (it == this->actions.end())
      BOOST_CHECK_EQUAL(true, false);
  }

  void
  readData(const Name& name, const repo::status& stat)
  {
    if (this->dataNames[name] != stat)
      BOOST_CHECK_EQUAL(true, false);
  }

  void
  readTree(const ActionEntry & action)
  {
    if (this->seqs[action.getCreatorName()] != action.getSeqNo())
      BOOST_CHECK_EQUAL(true, false);
  }
};

BOOST_FIXTURE_TEST_CASE_TEMPLATE(Bulk, T, ActionSets, MsgFixture<T>)
{
  BOOST_TEST_MESSAGE(T::getName());

  Msg message1(SyncStateMsg::ACTION);
  typename T::ActionContainer::iterator i = this->actions.begin();
  message1.writeActionToMsg(*i);
  int len = message1.getMsg().ByteSize();
  char *wireData = new char[len];
  message1.getMsg().SerializeToArray(wireData, len);
  SyncStateMsg msg;
  if (!msg.ParseFromArray(wireData, len) || !msg.IsInitialized())
  {
    BOOST_THROW_EXCEPTION(SyncStateMsgDecodingFailure());
  }
  Msg message2(msg);
  BOOST_CHECK_EQUAL(message2.getMsg().type(), SyncStateMsg::ACTION);
  message2.readActionFromMsg(bind(&MsgFixture<T>::readAction, this, _1));
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(Bulk1, T, ActionSets, MsgFixture<T>)
{
  BOOST_TEST_MESSAGE(T::getName());

  Msg message1(SyncStateMsg::ACTION);
  for (typename T::ActionContainer::iterator i = this->actions.begin(); i != this->actions.end(); i++) {
    message1.writeActionNameToMsg(*i);
  }
  int len = message1.getMsg().ByteSize();
  char *wireData = new char[len];
  message1.getMsg().SerializeToArray(wireData, len);
  SyncStateMsg msg;
  if (!msg.ParseFromArray(wireData, len) || !msg.IsInitialized())
  {
    BOOST_THROW_EXCEPTION(SyncStateMsgDecodingFailure());
  }
  Msg message2(msg);
  BOOST_CHECK_EQUAL(message2.getMsg().type(), SyncStateMsg::ACTION);
  message2.readActionNameFromMsg(bind(&MsgFixture<T>::readActionName, this, _1, _2), "/useless");
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(Bulk2, T, ActionSets, MsgFixture<T>)
{
  BOOST_TEST_MESSAGE(T::getName());

  Msg message1(SyncStateMsg::SNAPSHOT);
  for (typename T::DataNameContainer::iterator i = this->dataNames.begin(); i != this->dataNames.end(); i++) {
    message1.writeDataToSnapshot(i->first, i->second);
  }
  for (typename T::SeqContainer::iterator i = this->seqs.begin(); i != this->seqs.end(); i++) {
    message1.writeTreeToSnapshot(i->first, i->second);
  }
  message1.writeInfoToSnapshot("/snapshot", 0);
  int len = message1.getMsg().ByteSize();
  char *wireData = new char[len];
  message1.getMsg().SerializeToArray(wireData, len);
  SyncStateMsg msg;
  if (!msg.ParseFromArray(wireData, len) || !msg.IsInitialized())
  {
    BOOST_THROW_EXCEPTION(SyncStateMsgDecodingFailure());
  }
  Msg message2(msg);
  BOOST_CHECK_EQUAL(message2.getMsg().type(), SyncStateMsg::SNAPSHOT);
  message2.readDataFromSnapshot(bind(&MsgFixture<T>::readData, this, _1, _2));
  message2.readTreeFromSnapshot(bind(&MsgFixture<T>::readTree, this, _1));
  std::pair<Name,uint64_t> info = message2.readInfoFromSnapshot();
  BOOST_CHECK_EQUAL(info.first, "/snapshot");
  BOOST_CHECK_EQUAL(info.second, 0);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace tests
} // namespace repo
