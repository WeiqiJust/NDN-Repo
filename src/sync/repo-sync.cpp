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

#include "repo-sync.hpp"

namespace repo {

const int syncResponseFreshness = 1000;
const int syncInterestReexpress = 4;
const int defaultRecoveryRetransmitInterval = 200; // milliseconds
const int retrytimes = 4;
const int pipeline = 3;
static const milliseconds DEFAULT_INTEREST_LIFETIME(4000);

static bool
compareDigest(std::pair<ndn::ConstBufferPtr, ActionEntry> entry, ndn::ConstBufferPtr digest)
{
  return *entry.first == *digest;
}

static bool
compareActionEntry(std::pair<ndn::ConstBufferPtr, ActionEntry> entry, const Name& name)
{
  return entry.second.getName() == name;
}

static bool
compareSnapshot(std::pair<Name, uint64_t> entry, std::pair<Name, uint64_t> info)
{
  return entry == info;
}

RepoSync::RepoSync(const Name& syncPrefix, const Name& creatorName, const std::string& dbPath,
                   Face& face, KeyChain& keyChain, ValidatorConfig& validator, RepoStorage& storageHandle)
  : m_syncPrefix(syncPrefix)
  , m_creatorName(creatorName)
  , m_seq(0)    // action sequence initiate as 0, the first action sequence is 1
  , m_isSynchronized(false)
  , m_isRunning(false)
  , m_face(face)
  , m_keyChain(keyChain)
  , m_scheduler(face.getIoService())
  , m_validator(validator)
  , m_storageHandle(storageHandle)
  , m_syncTree(dbPath)
  , m_recoveryRetransmissionInterval(defaultRecoveryRetransmitInterval)
  , m_randomGenerator(static_cast<unsigned int>(std::time(0)))
  , m_rangeUniformRandom(m_randomGenerator, boost::uniform_int<>(200,1000))
  , m_reexpressionJitter(m_randomGenerator, boost::uniform_int<>(100,500))
  , m_syncInterestTable(face.getIoService(), seconds(syncInterestReexpress))
  , m_snapshot(SyncStateMsg::SNAPSHOT)
  , m_snapshotNo(0)
{
  init();
}

RepoSync::~RepoSync()
{
  m_scheduler.cancelEvent(m_reexpressingInterestId);
}

void
RepoSync::init()
{
  m_actionList.clear();
  Name rootName("/");
  ActionEntry entry(rootName, -1);
  m_actionList.push_back(std::make_pair(m_syncTree.getDigest(), entry));
  createSnapshot();
}

void
RepoSync::listen(const Name& prefix)
{
  Name syncCommandPrefix(prefix);
  syncCommandPrefix.append("sync");
  m_face.registerPrefix(syncCommandPrefix,
                           bind(&RepoSync::onRegistered, this, _1),
                           bind(&RepoSync::onRegisterFailed, this, _1, _2));
}

void
RepoSync::onRegistered(const Name& prefix)
{
  m_face.setInterestFilter(Name().append(prefix).append("start"),
                              bind(&RepoSync::onInterest, this, _1, _2));
  m_face.setInterestFilter(Name().append(prefix).append("check"),
                              bind(&RepoSync::onCheckInterest, this, _1, _2));
  m_face.setInterestFilter(Name().append(prefix).append("stop"),
                              bind(&RepoSync::onStopInterest, this, _1, _2));
}

void
RepoSync::onRegisterFailed(const Name& prefix, const std::string& reason)
{
  std::cerr << reason << std::endl;
  throw Error("Sync Command Prefix Registration Failed");
}

void
RepoSync::onInterest(const Name& prefix, const Interest& interest)
{
  //std::cout<<"creator = "<<m_creatorName<<" on command interest prefix = "<<prefix<<std::endl;
  m_validator.validate(interest,
                       bind(&RepoSync::onValidated, this, _1, prefix),
                       bind(&RepoSync::onValidationFailed, this, _1, _2));
}

void
RepoSync::onValidated(const shared_ptr<const Interest>& interest, const Name& prefix)
{
  if (m_isRunning)
    reply(*interest, 300);
  m_face.setInterestFilter(m_syncPrefix,
                           bind(&RepoSync::onSyncInterest, this, _1, _2),
                           bind(&RepoSync::onSyncRegisterFailed, this, _1, _2));
  m_reexpressingInterestId = m_scheduler.scheduleEvent(milliseconds(100),
                                                       bind(&RepoSync::sendSyncInterest, this));
  RepoCommandParameter parameter;
  try {
    parameter.wireDecode(interest->getName().get(prefix.size()).blockFromValue());
  }
  catch (RepoCommandParameter::Error) {
    reply(*interest, 403);
    return;
  }
  if (parameter.hasName()) {
    m_creatorName = parameter.getName();
    m_creatorName.appendNumber(ndn::random::generateWord64());
  }
  reply(*interest, 100);  // code 100, successfully start sync
  m_isRunning = true;
}

void
RepoSync::onValidationFailed(const shared_ptr<const Interest>& interest, const string& reason)
{
  std::cerr << reason << std::endl;
  reply(*interest, 401);  //code 401, command interest validation failed
}

void
RepoSync::onCheckInterest(const Name& prefix, const Interest& interest)
{
  m_validator.validate(interest,
                       bind(&RepoSync::onCheckValidated, this, _1, prefix),
                       bind(&RepoSync::onValidationFailed, this, _1, _2));
}

void
RepoSync::onCheckValidated(const shared_ptr<const Interest>& interest, const Name& prefix)
{
  RepoCommandParameter parameter;
  try {
    parameter.wireDecode(interest->getName().get(prefix.size()).blockFromValue());
  }
  catch (RepoCommandParameter::Error) {
    reply(*interest, 403);
    return;
  }
  if (m_isRunning)
    reply(*interest, 200); //code 200, sync is running
  else
    reply(*interest, 300); //code 300, sync is stopped
}

void
RepoSync::onStopInterest(const Name& prefix, const Interest& interest)
{
  m_validator.validate(interest,
                       bind(&RepoSync::onStopValidated, this, _1, prefix),
                       bind(&RepoSync::onValidationFailed, this, _1, _2));
}

void
RepoSync::onStopValidated(const shared_ptr<const Interest>& interest, const Name& prefix)
{
  RepoCommandParameter parameter;
  try {
    parameter.wireDecode(interest->getName().get(prefix.size()).blockFromValue());
  }
  catch (RepoCommandParameter::Error) {
    reply(*interest, 403);
    return;
  }
  m_isRunning = false;
  reply(*interest, 300);
}

void
RepoSync::reply(const Interest& commandInterest, const int statusCode)
{
  RepoCommandResponse response;
  response.setStatusCode(statusCode);
  shared_ptr<Data> rdata = make_shared<Data>(commandInterest.getName());
  rdata->setContent(response.wireEncode());
  m_keyChain.sign(*rdata);
  m_face.put(*rdata);
}

Action
RepoSync::strToAction(const std::string& action)
{
  if (action == "insertion") {
   return INSERTION;
  }
  else if (action == "deletion") {
    return DELETION;
  }
  else {
    throw Error("Action type is wrong. No sucn action!");
  }
  return OTHERS;
}

// insert action into actionlist from repo handles
void
RepoSync::insertAction(const Name& dataName, const std::string& status)
{
  m_seq++;
  Action action = strToAction(status);
  ActionEntry entry(m_creatorName, dataName, action);
  uint64_t version = ++m_seqIndex[std::make_pair(dataName, action)];
  entry.setVersion(version);
  entry.setSeqNo(m_seq);
  entry.constructName();
  m_syncTree.update(entry);
  m_actionList.push_back(std::make_pair(m_syncTree.getDigest(), entry));
  m_nodeSeq[m_creatorName].current = m_seq;
  m_nodeSeq[m_creatorName].final = m_seq;
  processPendingSyncInterests();
}

void
RepoSync::printSyncStatus(ndn::function< void (const Name &, const uint64_t &) > f)
{
  for (SyncTree::const_iter iter = m_syncTree.begin(); iter != m_syncTree.end(); iter++) {
    f(iter->first, iter->second.last);
  }
}

uint64_t
RepoSync::printSyncStatus(const Name& name)
{
  SyncTree::const_iter iterator = m_syncTree.lookup(name);
  if (iterator != m_syncTree.end())
    return iterator->second.last;
  else
    return 0;
}

void
RepoSync::onSyncInterest(const Name& prefix, const Interest& interest)
{
  if (!m_isRunning)
    return;
  //std::cout<<m_creatorName<<"  on sync interest"<<std::endl;
  Name name = interest.getName();
  int nameLengthDiff = name.size() - m_syncPrefix.size();
  // syncInterest /ndn/broadcast/sync/digest
  // fetchInterest /ndn/broadcast/fetch/creatorName/seq
  // recoveryInterest /ndn/broadcast/recovery/digest
  BOOST_ASSERT(nameLengthDiff > 1);

  try
    {
      std::string type = name[m_syncPrefix.size()].toUri();
      if (type == "sync")
        {
          ndn::ConstBufferPtr digest = convertNameToDigest(name);
          processSyncInterest(name, digest, false);
        }
      else if (type == "fetch")
        {
          processFetchInterest(name);
        }
      else if (type == "recovery")
        {
          ndn::ConstBufferPtr digest = convertNameToDigest(name);
          processRecoveryInterest(name, digest);
        }
       else
         {
           throw Error("The interest type is not supported!");
         }
    }
  catch(DigestCalculationError &e)
    {
      throw Error("Something fishy happened...");
      return ;
    }
}

void
RepoSync::onSyncRegisterFailed(const Name& prefix, const std::string& msg)
{
  std::cerr <<  "Sync Register Fialed! " << std::endl;
}

ndn::ConstBufferPtr
RepoSync::convertNameToDigest(const Name &name)
{
  BOOST_ASSERT(m_syncPrefix.isPrefixOf(name));

  return make_shared<ndn::Buffer>(name.get(-1).value(), name.get(-1).value_size());
}

void
RepoSync::processSyncInterest(const Name& name, ndn::ConstBufferPtr digest, bool timeProcessing)
{
  if (!m_isRunning)
    return;
  ndn::ConstBufferPtr rootDigest = m_syncTree.getDigest();
  if (*rootDigest == *digest) {
    if (!m_isSynchronized) {
      //remove actions when no different digest received for a while
      m_synchronizedId = m_scheduler.scheduleEvent(seconds(5), bind(&RepoSync::removeActions, this));
      m_isSynchronized = true;
    }
    m_syncInterestTable.insert(digest, name, false);
    return;
  }
  // if received a different digest, cancel the event of removeActions
  m_scheduler.cancelEvent(m_synchronizedId);
  m_isSynchronized = false;
  std::list<std::pair<ndn::ConstBufferPtr, ActionEntry> >::iterator it = std::find_if(m_actionList.begin(),
                                                                            m_actionList.end(),
                                                                            bind(&compareDigest, _1, digest));
  // if the digest can be recognized, it means that the digest of the sender repo is outdated
  // return all the missing actions to the sender repo so that it can start to fetch the actions
  if (it != m_actionList.end()) {
    Msg message(SyncStateMsg::ACTION);
    ++it;
    while (it != m_actionList.end()) {
      message.writeActionNameToMsg(it->second);
      ++it;
    }
    sendData(name, message);
    checkInterestSatisfied(name);
    return;
  }
  // if the digest cannot be recognized, wait for a period of time to process this sync interest since
  // actions may be on the way to this repo
  // if after a period of time, the digest is still unrecognized, go to recovery process
  if (!timeProcessing)
    {
      bool exists = m_syncInterestTable.insert(digest, name, true);
      if (exists) // somebody else replied, so restart random-game timer
        {
          m_scheduler.cancelEvent(m_delayedInterestProcessingId);
        }
      uint32_t waitDelay = m_rangeUniformRandom();
      m_delayedInterestProcessingId = m_scheduler.scheduleEvent(milliseconds(waitDelay),
                                                                 bind(&RepoSync::processSyncInterest,
                                                                      this, name, digest, true));
    }
  else
    {
      m_syncInterestTable.remove(digest);
      m_recoveryRetransmissionInterval = defaultRecoveryRetransmitInterval;
      sendRecoveryInterest(digest);
    }
}

void
RepoSync::processFetchInterest(const Name& name)
{
  if (!m_isRunning)
    return;
  // if received fetch interest, the group is not synchronized, cancel the event of removeActions
  m_isSynchronized = false;
  m_scheduler.cancelEvent(m_synchronizedId);
  Name actionName = name.getSubName(m_syncPrefix.size() + 1);
  uint64_t seq = actionName.get(-1).toNumber();
  Name creator = actionName.getSubName(0, actionName.size() - 1);
  //std::cout<<m_creatorName<<" process Fetch interest name = "<<name<<std::endl;
  // check the sync tree to get the status of action's creator
  // if the requested action is removed, return the snapshot
  // Otherwise, send the action back
  SyncTree::const_iter iterator = m_syncTree.lookup(creator);
  //std::cout<<" before send snapshot creator = "<<creator<<" seq = "<<seq<<" first = "<<iterator->second.first<<std::endl;
  if (iterator != m_syncTree.end() && seq <= iterator->second.first && iterator->second.first != 0) {
    //std::cout<<m_creatorName<<"node information seq = "<<iterator->second.first<<" request seq = "<<seq<<std::endl;
    sendSnapshot(name);
    return;
  }
  std::list<std::pair<ndn::ConstBufferPtr, ActionEntry> >::iterator it =
                                        std::find_if(m_actionList.begin(), m_actionList.end(),
                                                     bind(&compareActionEntry, _1, actionName));
  if (it != m_actionList.end()) {
    Msg message(SyncStateMsg::ACTION);
    message.writeActionToMsg(it->second);
    sendData(name, message);
  }
}

void
RepoSync::processRecoveryInterest(const Name& name, ndn::ConstBufferPtr digest)
{
  if (!m_isRunning)
    return;
  //std::cout<<m_creatorName<<"  process recovery interest"<<std::endl;
  // if received recovery interest, the group is not synchronized, cancel the event of removeActions
  m_isSynchronized = false;
  m_scheduler.cancelEvent(m_synchronizedId);
  // check into action list to see whether this digest has once appeared or not
  // if the digest can be recognized, send back the current status of all the known nodes
  // Otherwise, ignore this interest
  std::list<std::pair<ndn::ConstBufferPtr, ActionEntry> >::iterator it =
                                                             std::find_if(m_actionList.begin(), m_actionList.end(),
                                                                          bind(&compareDigest, _1, digest));
  if (it != m_actionList.end()) {
    Msg message(SyncStateMsg::ACTION);
    SyncTree::const_iter iterator = m_syncTree.begin();
    while (iterator != m_syncTree.end()) {
      ActionEntry entry(iterator->first, iterator->second.last);
      message.writeActionNameToMsg(entry);
      ++iterator;
    }
    sendData(name, message);
    checkInterestSatisfied(name);
  }
}

void
RepoSync::sendSnapshot(const Name& name)
{
  //std::cout<<m_creatorName<<" send snapshot"<<std::endl;
  sendData(name, m_snapshot);
}

void
RepoSync::writeDataToSnapshot(Msg* msg, const Name& name, const status& stat)
{
  //std::cout<<"snapshot name = "<<name<<std::endl;
  msg->writeDataToSnapshot(name, stat);
}

void
RepoSync::sendSyncInterest()
{
  if (!m_isRunning)
    return;
  //std::cout<<m_creatorName<<"**************send sync interest**************  action size() =  "<<m_actionList.size()<<std::endl;
  //std::cout<<m_creatorName<<"interest digest is "<<ndn::name::Component(m_syncTree.getDigest())<<std::endl;
  m_outstandingInterestName = m_syncPrefix;
  m_outstandingInterestName.append(Name("sync")).append(ndn::name::Component(m_syncTree.getDigest()));

  ndn::Interest interest(m_outstandingInterestName);
  interest.setMustBeFresh(true);

  m_face.expressInterest(interest,
                         bind(&RepoSync::onData, this, _1, _2),
                         bind(&RepoSync::onSyncTimeout, this, _1));

  ndn::EventId eventId = m_scheduler.scheduleEvent(seconds(syncInterestReexpress) +
                                                      milliseconds(m_reexpressionJitter()),
                                                   bind(&RepoSync::sendSyncInterest, this));
  m_scheduler.cancelEvent(m_reexpressingInterestId);
  m_reexpressingInterestId = eventId;
}

void
RepoSync::sendFetchInterest(const Name& creatorName, const uint64_t& seq)
{
  if (!m_isRunning)
    return;
  Name actionName = creatorName;
  actionName.appendNumber(seq);
  //std::cout<<m_creatorName<<"send fetch interest name = "<<actionName<<" number = "<<m_retryTable[actionName]<<std::endl;
  // if the retry number of this fetch interest exceeds a certain value, stop fetching
  if (m_retryTable[actionName] >= retrytimes) {
    m_retryTable.erase(actionName);
    throw Error("Cannot fetch the aciton");
  }

  Name interestName = m_syncPrefix;
  Interest interest(interestName.append(Name("fetch")).append(creatorName).appendNumber(seq));
  interest.setMustBeFresh(true);

  m_face.expressInterest(interest,
                         bind(&RepoSync::onData, this, _1, _2), // to be implmented
                         bind(&RepoSync::onFetchTimeout, this, _1, creatorName, seq));
  m_retryTable[actionName]++;
}

void
RepoSync::sendRecoveryInterest(ndn::ConstBufferPtr digest)
{
  //std::cout<<m_creatorName<<"send recovery interest"<<std::endl;
  if (!m_isRunning)
    return;
  Name interestName = m_syncPrefix;
  interestName.append("recovery").append(ndn::name::Component(digest));

  system_clock::Duration nextRetransmission =
                milliseconds(m_recoveryRetransmissionInterval + m_reexpressionJitter());

  m_recoveryRetransmissionInterval <<= 1;

  m_scheduler.cancelEvent(m_reexpressingRecoveryInterestId);
  if (m_recoveryRetransmissionInterval < 100*1000) // <100 seconds
    m_reexpressingRecoveryInterestId = m_scheduler.scheduleEvent(nextRetransmission,
                                                                 bind(&RepoSync::sendRecoveryInterest,
                                                                 this, digest));

  ndn::Interest interest(interestName);
  interest.setMustBeFresh(true);

  m_face.expressInterest(interest,
                         bind(&RepoSync::onData, this, _1, _2),
                         bind(&RepoSync::onRecoveryTimeout, this, _1));
}


void
RepoSync::onSyncTimeout(const Interest& interest)
{
  //std::cerr << "Sync interest timeout"<<std::endl;
  // It is OK. Others will handle the time out situation.
}


void
RepoSync::onFetchTimeout(const Interest& interest, const Name& creatorName, const uint64_t& seq)
{
  //std::cerr << "Fetch interest timeout" <<std::endl;
  sendFetchInterest(creatorName, seq);
}

void
RepoSync::onRecoveryTimeout(const ndn::Interest& interest)
{
  //std::cerr << "Recovery interest timeout" <<std::endl;
}

void
RepoSync::checkInterestSatisfied(const Name &name)
{
  // checking if our own interest got satisfied
  // if satisfied schedule the event the resend the sync interest
  // std::cout<<"interest satisfied"<<std::endl;
  bool satisfiedOwnInterest = (m_outstandingInterestName == name);
  if (satisfiedOwnInterest)
    {
      system_clock::Duration after = milliseconds(m_reexpressionJitter());
      // std::cout << "------------ reexpress interest after: " << after << endl;
      ndn::EventId eventId = m_scheduler.scheduleEvent(after,
                                                       bind(&RepoSync::sendSyncInterest, this));
      m_scheduler.cancelEvent(m_reexpressingInterestId);
      m_reexpressingInterestId = eventId;
    }
}

void
RepoSync::sendData(const Name &name, Msg& ssm)
{
  //std::cout<<m_creatorName<<"on send data = "<<name<<std::endl;
  int size = ssm.getMsg().ByteSize();
  char *wireData = new char[size];
  ssm.getMsg().SerializeToArray(wireData, size);

  shared_ptr<Data> data = make_shared<Data>(name);
  data->setContent(reinterpret_cast<const uint8_t*>(wireData), size);
  data->setFreshnessPeriod(milliseconds(syncResponseFreshness));

  m_keyChain.sign(*data);

  m_face.put(*data);

  delete []wireData;

}

void
RepoSync::onData(const ndn::Interest& interest, Data& data)
{
  m_validator.validate(data,
                       bind(&RepoSync::onDataValidated, this, _1),
                       bind(&RepoSync::onDataValidationFailed, this, _1, _2));
}

void
RepoSync::onDataValidationFailed(const shared_ptr<const Data>& data, const std::string& reason)
{
  std::cerr << "Data cannot be verified! reason = " << reason << std::endl;
}

void
RepoSync::onDataValidated(const shared_ptr<const Data>& data)
{
  Name name = data->getName();

  const char* wireData = (const char*)data->getContent().value();
  size_t len = data->getContent().value_size();

  try
    {
      std::string type = name[m_syncPrefix.size()].toUri();
      if (type == "sync")
        {
          ndn::ConstBufferPtr digest = convertNameToDigest(name);
          m_syncInterestTable.remove(digest);
          processSyncData(name, wireData, len);
        }
      else if (type == "fetch")
        {
          processFetchData(name, wireData, len);
        }
      else if (type == "recovery")
        {
          ndn::ConstBufferPtr digest = convertNameToDigest(name);
          // timer is always restarted when we schedule recovery
          m_scheduler.cancelEvent(m_reexpressingRecoveryInterestId);
          m_syncInterestTable.remove(digest);
          processRecoveryData(name, wireData, len);
        }
    }
  catch(DigestCalculationError &e)
    {
      throw Error("Something fishy happened...");
      return;
    }
}

void
RepoSync::processSyncData(const Name& name, const char* wireData, size_t len)
{
  bool ownInterestSatisfied = false;
  ownInterestSatisfied = (name == m_outstandingInterestName);
  SyncStateMsg msg;
  //std::cout<<"process sync data = "<<name<<std::endl;
  if (!msg.ParseFromArray(wireData, len) || !msg.IsInitialized())
  {
    //Throw
    BOOST_THROW_EXCEPTION(SyncStateMsgDecodingFailure());
  }
  Msg message(msg);
  if (message.getMsg().type() == SyncStateMsg::ACTION) {
    message.readActionNameFromMsg(bind(&RepoSync::prepareFetchForSync, this, _1, _2), m_creatorName);
  }
  else {
    throw Error("The response of sync interest should not in this type!");
  }
  if (ownInterestSatisfied)
  {
    system_clock::Duration after = milliseconds(5000);
    //system_clock::Duration after = milliseconds(m_reexpressionJitter());
    // //std::cout << "------------ reexpress interest after: " << after << std::endl;
    ndn::EventId eventId = m_scheduler.scheduleEvent(after,
                                                     bind(&RepoSync::sendSyncInterest, this));
    m_scheduler.cancelEvent(m_reexpressingInterestId);
    m_reexpressingInterestId = eventId;
  }
}

void
RepoSync::processFetchData(const Name& name, const char* wireData, size_t len)
{
  SyncStateMsg msg;
  if (!msg.ParseFromArray(wireData, len) || !msg.IsInitialized())
  {
    //Throw
    BOOST_THROW_EXCEPTION(SyncStateMsgDecodingFailure() );
  }
  Msg message(msg);
  if (message.getMsg().type() == SyncStateMsg::ACTION) {
    // process action
    message.readActionFromMsg(bind(&RepoSync::actionControl, this, _1));
  }
  else if (message.getMsg().type() == SyncStateMsg::SNAPSHOT) {
    // process snapshot
    std::pair<Name, uint64_t> info = message.readInfoFromSnapshot();

    std::list<std::pair<Name, uint64_t> >::iterator it =
                                        std::find_if(m_snapshotList.begin(), m_snapshotList.end(),
                                                     bind(&compareSnapshot, _1, info));
    // if snapshot has been fetched once, ignore it
    // Otherwise, process the snapshot and record it info
    if (it != m_snapshotList.end()) {
      return;
    }
    m_snapshotList.push_back(info);
    m_scheduler.scheduleEvent(seconds(10),
                              bind(&RepoSync::removeSnapshotEntry, this, m_snapshotList.back()));

    message.readDataFromSnapshot(bind(&RepoSync::processSnapshot, this, _1, _2));
    message.readTreeFromSnapshot(bind(&RepoSync::updateSyncTree, this, _1));
  }
  else {
    throw Error("The response of fetch interest should not in this type!");
  }
}

void
RepoSync::processRecoveryData(const Name& name, const char* wireData, size_t len)
{
  SyncStateMsg msg;
  if (!msg.ParseFromArray(wireData, len) || !msg.IsInitialized())
  {
    //Throw
    BOOST_THROW_EXCEPTION(SyncStateMsgDecodingFailure() );
  }
  Msg message(msg);
  message.readActionNameFromMsg(bind(&RepoSync::prepareFetchForRecovery, this, _1, _2), m_creatorName);
}

void
RepoSync::prepareFetchForSync(const Name& name, const uint64_t seq)
{
  // this function is called when the sync interest digest is outdated
  // m_nodeSeq record the information of sequence number for each node
  // 'current' represents the last seq number the repo has
  // 'sending' represents the action seq number that is on fetching
  // 'final'   represents the last seq number that should be fetched
  SyncTree::const_iter iterator = m_syncTree.lookup(name);
  m_nodeSeq[name].final = seq;
  uint64_t& sending = m_nodeSeq[name].sending;
  if (iterator != m_syncTree.end())
  {
    m_nodeSeq[name].current = iterator->second.last;
    if (iterator->second.last >= seq || sending >= seq) {
      std::cerr << "Action has been fetched or sent" << std::endl;
      return;
    }
    uint64_t lastSendSeq = (sending+pipeline < seq ? sending+pipeline : seq);
    for (uint64_t seqno = sending + 1; seqno <= lastSendSeq; seqno++) {
      sendFetchInterest(name, seqno);
    }
    sending = lastSendSeq;
  }
  else
  {
    m_nodeSeq[name].current = 0;
    m_syncTree.addNode(name);
    uint64_t lastSendSeq = (pipeline < seq ? pipeline : seq);
    for (uint64_t seqno = 1; seqno <= lastSendSeq; seqno++) {
      sendFetchInterest(name, seqno);
    }
    sending = lastSendSeq;
  }
}

void
RepoSync::prepareFetchForRecovery(const Name& name, const uint64_t seq)
{
  // this function is called when the sync interest digest is unrecognized
  SyncTree::const_iter iterator = m_syncTree.lookup(name);
  m_nodeSeq[name].final = seq;
  //std::cout<<"prepare fetch for recovery name ="<<name<<" seq = "<<m_nodeSeq[name].final<<std::endl;
  if (iterator != m_syncTree.end())
  {
    m_nodeSeq[name].current = iterator->second.last;
    if (iterator->second.last >= seq) {
      std::cerr << "Action has been fetched" << std::endl;
      return;
    }
    uint64_t lastSendSeq = (iterator->second.last+pipeline < seq ? iterator->second.last+pipeline : seq);
    for (uint64_t seqno = iterator->second.last + 1; seqno <= lastSendSeq; seqno++) {
      sendFetchInterest(name, seqno);
    }
    m_nodeSeq[name].sending = lastSendSeq;
  }
  else
  {
    m_nodeSeq[name].current = 0;
    m_syncTree.addNode(name);
    uint64_t lastSendSeq = (pipeline < seq ? pipeline : seq);
    for (uint64_t seqno = 1; seqno <= lastSendSeq; seqno++) {
      sendFetchInterest(name, seqno);
    }
    m_nodeSeq[name].sending = lastSendSeq;
  }

}

void
RepoSync::processSnapshot(const Name& name, const status& dataStatus)
{
  status stat = m_storageHandle.getDataStatus(name);
  if (dataStatus == EXISTED) {
    if (stat == NONE) {  //if data is deleted, do not insert this data back
      sendNormalInterest(name);
    }
  }
  else if (dataStatus == DELETED) {
    //if data is inserted, means this data has once been deleted and inserted again,
    // so do not deleted the data.
    //we assume same data will not be deleted and inserted multiple times
    if (stat == EXISTED) {
      m_storageHandle.deleteData(name);
    }
  }
  else {
    // if data status is INSERTED, update the deleted data
    if (stat == NONE || stat == DELETED) {
      sendNormalInterest(name);
    }
  }
}

void
RepoSync::sendNormalInterest(const Name& name)
{
  Interest fetchInterest(name);
  fetchInterest.setInterestLifetime(DEFAULT_INTEREST_LIFETIME);
  m_face.expressInterest(fetchInterest,
                         bind(&RepoSync::onFetchData, this, _1, _2),
                         bind(&RepoSync::onDataTimeout, this, _1));
}

void
RepoSync::actionControl(const ActionEntry& action)
{
  // use pipeline to control received action
  // if receives an action, send the action with seq number = min( received seq + pipeline, final )
  // if received action is in ordered, apply the action
  // Otherwise, save it in the pending table and retransmit all the missing actions
  uint64_t& currentSeq = m_nodeSeq[action.getCreatorName()].current;
  uint64_t lastSeq = m_nodeSeq[action.getCreatorName()].final;
  if (action.getSeqNo() > lastSeq) {
    //std::cout<<"action name = "<<action.getCreatorName()<<" lastseq = "<<lastSeq<<std::endl;
    throw Error("Received unrecognized sequence number ");
    return;
  }
  Name name = action.getCreatorName();
  std::list<ActionEntry>& pendingList = m_pendingActionList[name];
  m_retryTable.erase(action.getName());
  if (currentSeq + 1 == action.getSeqNo()) {
    currentSeq++;
    applyAction(action);
    while (!pendingList.empty() && pendingList.front().getSeqNo() == currentSeq + 1) {
      currentSeq++;
      applyAction(pendingList.front());
      pendingList.pop_front();
    }

    if (currentSeq + pipeline <= lastSeq) {
      sendFetchInterest(name, action.getSeqNo() + pipeline);
      m_nodeSeq[name].sending = action.getSeqNo() + pipeline;
    }
  }
  else if (currentSeq + 1 < action.getSeqNo()) {
    // retransmit
    pendingList.push_back(action);
    pendingList.sort();
    for (uint64_t seqno = currentSeq + 1; seqno < pendingList.front().getSeqNo(); ++seqno) {
      sendFetchInterest(action.getCreatorName(), seqno);
    }
    m_nodeSeq[name].sending = pendingList.front().getSeqNo() - 1;
  }
  else {
    // do nothing
  }
}

void
RepoSync::applyAction(const ActionEntry& action)
{
  m_syncTree.update(action);
  // std::cout<<"update applyaction digest is = "<<m_syncTree.getDigest()<<std::endl;;
  m_actionList.push_back(std::make_pair(m_syncTree.getDigest(), action));
  if (action.getAction() == INSERTION) {
    Interest fetchInterest(action.getDataName());
    fetchInterest.setInterestLifetime(DEFAULT_INTEREST_LIFETIME);
    m_face.expressInterest(fetchInterest,
                           bind(&RepoSync::onFetchData, this, _1, _2),
                           bind(&RepoSync::onDataTimeout, this, _1));
  }
  else if (action.getAction() == DELETION) {
    m_storageHandle.deleteData(action.getDataName());
  }
  else {
    throw Error("Cannot apply this action type !");
  }
}

void
RepoSync::onFetchData(const Interest& interest, ndn::Data& data)
{
  m_validator.validate(data,
                       bind(&RepoSync::onFetchDataValidated, this, _1),
                       bind(&RepoSync::onDataValidationFailed, this, _1, _2));
}

void
RepoSync::onFetchDataValidated(const shared_ptr<const Data>& data)
{
  //std::cout<<"insert data name = "<<data->getName()<<std::endl;
  m_storageHandle.insertData(*data);
}

void
RepoSync::onDataTimeout(const Interest& interest)
{
  //std::cerr << "Fetch data timeout !" << std::endl;
}

void
RepoSync::processPendingSyncInterests()
{
  while (m_syncInterestTable.size() > 0)
  {
    ConstPendingInterestPtr interest = m_syncInterestTable.begin();
    processSyncInterest(interest->m_name, interest->m_digest, false);
    m_syncInterestTable.pop();
  }
}

void
RepoSync::removeActions()
{
  init();
  m_retryTable.clear();
  m_pendingActionList.clear();
  createSnapshot();
}

void
RepoSync::createSnapshot()
{
  //std::cout<<m_creatorName<<" createSnapshot seq = "<<m_snapshotNo<<""<<std::endl;
  Msg message(SyncStateMsg::SNAPSHOT);
  m_storageHandle.dataEnumeration(bind(&RepoSync::writeDataToSnapshot, this, &message, _1, _2));
  for (SyncTree::const_iter iter = m_syncTree.begin(); iter != m_syncTree.end(); iter++) {
    message.writeTreeToSnapshot(iter->first, iter->second.last);
  }
  message.writeInfoToSnapshot(m_creatorName, m_snapshotNo);
  m_snapshot.setMsg(message.getMsg());
  m_snapshotNo++;
  m_syncTree.updateForSnapshot();
}

void
RepoSync::updateSyncTree(const ActionEntry& entry)
{
  m_syncTree.update(entry);
  pipelineEntrySeq &node = m_nodeSeq[entry.getCreatorName()];
  node.current = entry.getSeqNo();
  node.sending = entry.getSeqNo();
  node.final = entry.getSeqNo() < node.final ? node.final : entry.getSeqNo();
}

void
RepoSync::removeSnapshotEntry(std::pair<Name, uint64_t> info)
{
  m_snapshotList.remove(info);
}

} // namespace repo
