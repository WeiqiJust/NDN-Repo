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

#include "common.hpp"

#include "sync/repo-sync.hpp"
#include "sync/action-entry.hpp"
#include "../repo-storage-fixture.hpp"
#include "storage/sqlite-storage.hpp"
#include "../action-fixture.hpp"

#include <ndn-cxx/util/random.hpp>
#include <ndn-cxx/util/io.hpp>

#include <boost/test/unit_test.hpp>
#include <fstream>
#include <stdlib.h>

namespace repo {
namespace tests {

using std::vector;
using std::map;

BOOST_AUTO_TEST_SUITE(SyncLogic)

template<class Actionset>
class Node : public RepoStorageFixture, public Actionset
{
public:
  Node(ndn::Face& face, const Name& creatorName, KeyChain& keyChain, std::string path)
    : validator(face)
    , sync("/ndn/broadcast", creatorName, path, face, keyChain, validator, *handle)
    , creator(creatorName)
    , dbPath(path)
  {
    validator.load("tests/integrated/insert-delete-validator-config.conf");
  }

  ~Node()
  {
    boost::filesystem::remove_all(boost::filesystem::path(dbPath));
  }

  std::string
  convertActionToStr(const Action& type) const
  {
    std::string str;
    switch (type) {
      case INSERTION:
        str = "insertion";
        break;
      case DELETION:
        str = "deletion";
        break;
      default:
        str = "other";
        BOOST_CHECK_EQUAL(true, false);
        std::cerr << "The type is not supported" << std::endl;
    }
    return str;
  }

  void
  insertAction(const ActionEntry& action)
  {
    std::string str = convertActionToStr(action.getAction());
    sync.insertAction(action.getDataName(), str);
  }

  void
  check(const Name& name, const uint64_t seq)
  {
    std::cout<<"check node = "<<creator<<" name = "<<name<<std::endl;
    BOOST_CHECK_EQUAL(sync.printSyncStatus(name), seq);
  }

  void
  startListen(const Name& prefix)
  {
    Name name = prefix;
    name.append(creator);
    sync.listen(name);
  }

private:
  ValidatorConfig validator;
  RepoSync sync;
  Name creator;
  std::string dbPath;
};

template<class T>
class LogicFixture
{
public:
  LogicFixture(ndn::shared_ptr<boost::asio::io_service> ioService)
    : m_ioService(ioService)
  {
    creator[0] = Name("/repo/0");
    creator[1] = Name("/repo/1");
    creator[2] = Name("/repo/2");
  }
  
  void
  createNode(int idx);

  void
  insertAction(int idx, const ActionEntry& action)
  {
    ActionEntry entry = node[idx]->createAction(action.getCreatorName(),
                                                action.getDataName(), action.getAction());
    node[idx]->insertAction(entry);
  }

  void
  checkSeqNo(int sIdx, int nodeId, uint64_t seq)
  {
    node[sIdx]->check(creator[nodeId], seq);
  }

  void
  terminate(ndn::shared_ptr<boost::asio::io_service> ioService)
  {
    ioService->stop();
  }

  void
  generateDefaultCertificateFile();

  void
  sendSyncStartInterest(int idx);

  void
  onData(const Interest& interest, Data& data);

  void
  onTimeout(const Interest& interest);

  Name creator[3];
  shared_ptr<Node<T> > node[3];
  ndn::shared_ptr<boost::asio::io_service> m_ioService;
  ndn::shared_ptr<ndn::Face> faces[3];
  ndn::shared_ptr<ndn::Face> receiveFaces[3];
  KeyChain keyChain;
};

template<class T> void
LogicFixture<T>::createNode(int idx)
{
  faces[idx] = make_shared<ndn::Face>(ndn::ref(*m_ioService));
  std::string path = "unittestdb_synctree" + boost::lexical_cast<std::string>(idx);
  node[idx] = boost::make_shared<Node<T> >(ndn::ref(*faces[idx]), creator[idx], ndn::ref(keyChain), path);
  node[idx]->startListen("/repo/command");
}

template<class T>  void
LogicFixture<T>::generateDefaultCertificateFile()
{
  Name defaultIdentity = keyChain.getDefaultIdentity();
  Name defaultKeyname = keyChain.getDefaultKeyNameForIdentity(defaultIdentity);
  Name defaultCertficateName = keyChain.getDefaultCertificateNameForKey(defaultKeyname);
  shared_ptr<ndn::IdentityCertificate> defaultCertficate = keyChain.getCertificate(defaultCertficateName);
  //test-integrated should run in root directory of repo-ng.
  //certificate file should be removed after tests for security issue.
  std::fstream certificateFile("tests/integrated/insert-delete-test.cert",
                               std::ios::out | std::ios::binary | std::ios::trunc);
  ndn::io::save(*defaultCertficate, certificateFile);
  certificateFile.close();
}

template<class T> void
LogicFixture<T>::sendSyncStartInterest(int idx)
{
  Name name("/repo/command");
  name.append(creator[idx]);
  name.append("sync").append("start");
  
  RepoCommandParameter parameter;
  //parameter.setName(Name("/ndn/example"));
  name.append(parameter.wireEncode());
  
  Interest syncInterest;
  syncInterest.setName(name);
  keyChain.signByIdentity(syncInterest, keyChain.getDefaultIdentity());
  receiveFaces[idx] = make_shared<ndn::Face>(ndn::ref(*m_ioService));
  receiveFaces[idx]->expressInterest(syncInterest,
                       bind(&LogicFixture<T>::onData, this, _1, _2),
                       bind(&LogicFixture<T>::onTimeout, this, _1));
}

template<class T> void
LogicFixture<T>::onData(const Interest& interest, Data& data)
{
}

template<class T> void
LogicFixture<T>::onTimeout(const Interest& interest)
{
  BOOST_ERROR("Sync Start command timeout");
}

BOOST_AUTO_TEST_CASE(SyncLogicTest)
{
  ndn::shared_ptr<boost::asio::io_service> ioService = ndn::make_shared<boost::asio::io_service>();
  ndn::Scheduler scheduler(*ioService);
  LogicFixture<ActionSet> sync(ioService);
  sync.generateDefaultCertificateFile();

  scheduler.scheduleEvent(ndn::time::milliseconds(100),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(1100),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(200),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(1200),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 1));
  
  ActionEntry entry1("/repo/0", "/example/data/1", INSERTION);
  scheduler.scheduleEvent(ndn::time::milliseconds(2000),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 0, entry1));


  ActionEntry entry2("/repo/1", "/example/data/2", INSERTION);

  scheduler.scheduleEvent(ndn::time::milliseconds(2060),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 1, entry2));

  scheduler.scheduleEvent(ndn::time::milliseconds(15000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(15010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(15020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(15030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(15100),
                                bind(&LogicFixture<ActionSet>::terminate, &sync, ioService));

  ioService->run();
  
}

BOOST_AUTO_TEST_CASE(ThreeBasic)
{
  ndn::shared_ptr<boost::asio::io_service> ioService = ndn::make_shared<boost::asio::io_service>();
  ndn::Scheduler scheduler(*ioService);
  LogicFixture<ActionSet> sync(ioService);
  sync.generateDefaultCertificateFile();

  scheduler.scheduleEvent(ndn::time::milliseconds(100),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(1100),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(200),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(1200),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 1));

  ActionEntry entry1("/repo/0", "/example/data/1", INSERTION);
  scheduler.scheduleEvent(ndn::time::milliseconds(1300),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 0, entry1));

  ActionEntry entry2("/repo/1", "/example/data/2", INSERTION);

  scheduler.scheduleEvent(ndn::time::milliseconds(1400),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 1, entry2));


  scheduler.scheduleEvent(ndn::time::milliseconds(5000),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(6000),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(16000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(16010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(16020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(16030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  ActionEntry entry3("/repo/2", "/example/data/3", INSERTION);
  scheduler.scheduleEvent(ndn::time::milliseconds(16000),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 0, entry3));

  scheduler.scheduleEvent(ndn::time::milliseconds(26000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(26010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(26020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 2, 0));
  
  scheduler.scheduleEvent(ndn::time::milliseconds(26030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(26040),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(26050),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 2, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(26060),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 0, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(26070),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(26080),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 2, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(26500),
                          bind(&LogicFixture<ActionSet>::terminate, &sync, ioService));

  ioService->run();
  boost::filesystem::remove_all(boost::filesystem::path("unittestdb_synctree"));
}

BOOST_AUTO_TEST_CASE(Snapshot)
{
  ndn::shared_ptr<boost::asio::io_service> ioService = ndn::make_shared<boost::asio::io_service>();
  ndn::Scheduler scheduler(*ioService);
  LogicFixture<ActionSet> sync(ioService);
  sync.generateDefaultCertificateFile();

  scheduler.scheduleEvent(ndn::time::milliseconds(100),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(1100),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(200),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(1200),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 1));

  ActionEntry entry1("/repo/0", "/example/data/1", INSERTION);
  scheduler.scheduleEvent(ndn::time::milliseconds(1300),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 0, entry1));

  ActionEntry entry2("/repo/1", "/example/data/2", INSERTION);

  scheduler.scheduleEvent(ndn::time::milliseconds(1400),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 1, entry2));

  scheduler.scheduleEvent(ndn::time::milliseconds(10000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(10010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(10020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(10030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(15000),
                          bind(&LogicFixture<ActionSet>::createNode, &sync, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(16010),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(30000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(30010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(30020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 2, 0));

  ActionEntry entry3("/repo/2", "/example/data/3", INSERTION);
  scheduler.scheduleEvent(ndn::time::milliseconds(30040),
                                bind(&LogicFixture<ActionSet>::insertAction, &sync, 2, entry3));

  scheduler.scheduleEvent(ndn::time::milliseconds(40000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 2, 1));
  
  scheduler.scheduleEvent(ndn::time::milliseconds(40030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40040),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40050),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 2, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40060),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 0, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40070),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 1, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40080),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 2, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(40500),
                          bind(&LogicFixture<ActionSet>::terminate, &sync, ioService));

  ioService->run();
  boost::filesystem::remove_all(boost::filesystem::path("unittestdb_synctree"));
}

BOOST_AUTO_TEST_CASE(pipeline)
{
  ndn::shared_ptr<boost::asio::io_service> ioService = ndn::make_shared<boost::asio::io_service>();
  ndn::Scheduler scheduler(*ioService);
  LogicFixture<ActionSet> sync(ioService);
  sync.generateDefaultCertificateFile();

  Name creator("/repo/0");
  Name dataBasic("/example/data");
  scheduler.scheduleEvent(ndn::time::milliseconds(100),
                                bind(&LogicFixture<ActionSet>::createNode, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(1100),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 0));

  for (uint64_t i = 0; i < 20; i++) {
    Name dataName = dataBasic;
    ActionEntry entry(creator, dataName.appendNumber(i), INSERTION);
    scheduler.scheduleEvent(ndn::time::milliseconds(200 + i * 100),
                            bind(&LogicFixture<ActionSet>::insertAction, &sync, 0, entry));
  }

  scheduler.scheduleEvent(ndn::time::milliseconds(3000),
                                bind(&LogicFixture<ActionSet>::createNode, &sync, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(4000),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(15000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 20));

  scheduler.scheduleEvent(ndn::time::milliseconds(15010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(15020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 20));

  scheduler.scheduleEvent(ndn::time::milliseconds(15030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 1, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(15500),
                                bind(&LogicFixture<ActionSet>::terminate, &sync, ioService));

  ioService->run();
  boost::filesystem::remove_all(boost::filesystem::path("unittestdb_synctree"));
}

BOOST_AUTO_TEST_CASE(Recovery)
{
  ndn::shared_ptr<boost::asio::io_service> ioService = ndn::make_shared<boost::asio::io_service>();
  ndn::Scheduler scheduler(*ioService);
  LogicFixture<ActionSet> sync(ioService);
  sync.generateDefaultCertificateFile();

  Name creator("/repo/0");
  Name dataBasic("/example/data");
  scheduler.scheduleEvent(ndn::time::milliseconds(100),
                                bind(&LogicFixture<ActionSet>::createNode, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(1500),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(110),
                                bind(&LogicFixture<ActionSet>::createNode, &sync, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(1600),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 1));

  for (uint64_t i = 0; i < 20; i++) {
    Name dataName = dataBasic;
    ActionEntry entry(creator, dataName.appendNumber(i), INSERTION);
    scheduler.scheduleEvent(ndn::time::milliseconds(200 + i * 100),
                            bind(&LogicFixture<ActionSet>::insertAction, &sync, 0, entry));
  }

  scheduler.scheduleEvent(ndn::time::milliseconds(3000),
                                bind(&LogicFixture<ActionSet>::createNode, &sync, 2));

  scheduler.scheduleEvent(ndn::time::milliseconds(4010),
                          bind(&LogicFixture<ActionSet>::sendSyncStartInterest, &sync, 2));

  ActionEntry entry1("/repo/2", dataBasic, INSERTION);
  scheduler.scheduleEvent(ndn::time::milliseconds(4020),
                          bind(&LogicFixture<ActionSet>::insertAction, &sync, 2, entry1));

  scheduler.scheduleEvent(ndn::time::milliseconds(20000),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 0, 20));

  scheduler.scheduleEvent(ndn::time::milliseconds(20010),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 1, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(20020),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 0, 2, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(20030),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 0, 20));

  scheduler.scheduleEvent(ndn::time::milliseconds(20040),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 1, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(20050),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 1, 2, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(20060),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 0, 20));

  scheduler.scheduleEvent(ndn::time::milliseconds(20070),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 1, 0));

  scheduler.scheduleEvent(ndn::time::milliseconds(20080),
                                bind(&LogicFixture<ActionSet>::checkSeqNo, &sync, 2, 2, 1));

  scheduler.scheduleEvent(ndn::time::milliseconds(20500),
                                bind(&LogicFixture<ActionSet>::terminate, &sync, ioService));

  ioService->run();
  boost::filesystem::remove_all(boost::filesystem::path("unittestdb_synctree"));
}

BOOST_AUTO_TEST_SUITE_END()

} //namespace tests
} //namespace repo
