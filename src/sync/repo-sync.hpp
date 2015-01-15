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

#ifndef REPO_SYNC_REPO_SYNC_HPP
#define REPO_SYNC_REPO_SYNC_HPP
#include <boost/random.hpp>
#include <boost/exception/all.hpp>
#include <ndn-cxx/util/digest.hpp>

#include "common.hpp"
#include "action-entry.hpp"
#include "sync-tree.hpp"
#include "sync-msg.hpp"

#include "storage/repo-storage.hpp"
#include "storage/index.hpp"
#include "sync-interest-table.hpp"
#include "repo-command-response.hpp"
#include "repo-command-parameter.hpp"

namespace repo {
using namespace ndn::time;

class RepoSync
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
  struct pipelineEntrySeq
  {
    uint64_t current;   // the action has already been fetched
    uint64_t sending;   // the last action being sent
    uint64_t final;     // the last action that should be fetched, used in recovery
  };

public:

  RepoSync(const Name& syncPrefix, const Name& creatorName, const std::string& dbPath,
           Face& face, KeyChain& keyChain, ValidatorConfig& validator, RepoStorage& storageHandle);

  ~RepoSync();

  /**
   * @brief  used to insert the actions that local repo generates according to command interests,
   *          this function is called by repo handles(delete, write, watch-prefix, tcp-insert)
   * @param  Name   the data name that action applies
   * @param  string action type(insertion, deletion)
   */
  void
  insertAction(const Name& dataName, const std::string& status);

  /**
   * @brief  print the infomation in sync tree with all the creator names and their sequence number
   */
  void
  printSyncStatus(ndn::function< void (const Name &, const uint64_t &) > f);

  /**
   * @brief  print the infomation in sync tree with certain the creator names and their sequence number
   */
  uint64_t
  printSyncStatus(const Name& name);

  void
  listen(const Name& prefix);

private:

  void
  onInterest(const Name& prefix, const Interest& interest);

  void
  onValidated(const shared_ptr<const Interest>& interest, const Name& prefix);

  void
  onValidationFailed(const shared_ptr<const Interest>& interest, const string& reason);

  void
  onRegistered(const Name& prefix);

  void
  onRegisterFailed(const Name& prefix, const std::string& reason);

  void
  onCheckInterest(const Name& prefix, const Interest& interest);

  void
  onCheckValidated(const shared_ptr<const Interest>& interest, const Name& prefix);

  void
  onStopInterest(const Name& prefix, const Interest& interest);

  void
  onStopValidated(const shared_ptr<const Interest>& interest, const Name& prefix);

  void
  reply(const Interest& commandInterest, const int statusCode);

private:
  /**
   * @brief  initiate the action list by inserting a root name to avoid empty digest
   *         and can be used for snapshot
   */
  void
  init();

  /**
   * @brief  the call back of successfully received and interest and call different kinds
   *         of interests procession respectivly
   */
  void
  onSyncInterest(const Name& prefix, const Interest& interest);

  /**
   * @brief  the call back of registering interest filter failed
   */
  void
  onSyncRegisterFailed(const Name& prefix, const std::string& msg);

  /**
   * @brief  use the interest name to construct digest
   */
  ndn::ConstBufferPtr
  convertNameToDigest(const Name &name);

  /**
   * @brief  transform the action type from std::string to Action
   */
  Action
  strToAction(const std::string& action);

  /**
   * @brief check whether own interest get satisfied
   * @param Name   interest name
   */
  void
  checkInterestSatisfied(const Name &name);

  /**
   * @brief  save the interests that cannot be processed now and release them
   *         when local status changes by generating an action
   */
  void
  processPendingSyncInterests();

  /**
   * @brief  remove all the actions in action list when has not received sync interest with
   *         different digest for a while. Create a snapshot after actions removed
   */
  void
  removeActions();

  /**
   * @brief  enumerate data in database and save all the data name in the snapshot
   */
  void
  writeDataToSnapshot(Msg* msg, const Name& name, const status& stat);

  /**
   * @brief  remove the snapshot info in the snapshot list, this fuction will be called
   *         after the snapshot is applied for a period of time
   */
  void
  removeSnapshotEntry(std::pair<Name, uint64_t> info);

  /**
   * @brief  create the snapshot, this function will be called by removeActions
   */
  void
  createSnapshot();

  /**
   * @brief  apply the data in the snapshot to local database, whether insert (fetch),
   *         delete the data or do nothing is based on the status of the data in snapshot and database
   */
  void
  processSnapshot(const Name& name, const status& dataStatus);

  /**
   * @brief  use the infomation of other nodes status to update the sync tree
   */
  void
  updateSyncTree(const ActionEntry& entry);

  ndn::ConstBufferPtr
  getDigest() const
  {
    return m_syncTree.getDigest();
  }

private:  // process different kinds of interests
  /**
   * @brief  process sync interest
   * @param  Name              interest name
   * @param  ConstBufferPtr    the smart pointer of digest obtained from interest name
   * @param  bool              used to indicate whether to process recovery process or not
   */
  void
  processSyncInterest(const Name& name, ndn::ConstBufferPtr digest, bool timeProcessing);

  /**
   * @brief  process sync interest
   * @param  Name              interest name
   */
  void
  processFetchInterest(const Name& name);

  /**
   * @brief  process sync interest
   * @param  Name              interest name
   * @param  ConstBufferPtr    the smart pointer of digest obtained from interest name
   */
  void
  processRecoveryInterest(const Name& name, ndn::ConstBufferPtr digest);

  void
  sendSnapshot(const Name& name);

  /**
   * @brief  send interest response back, called by interests procession
   * @param  Name  data name
   * @param  Msg   the information data contain
   */
  void
  sendData(const Name &name, Msg& ssm);

private:  // send different kinds of interests

  void
  sendSyncInterest();

  void
  sendFetchInterest(const Name& creatorName, const uint64_t& seq);

  void
  sendRecoveryInterest(ndn::ConstBufferPtr digest);

  void
  onSyncTimeout(const Interest& interest);

  void
  onFetchTimeout(const Interest& interest, const Name& creatorName, const uint64_t& seq);

  void
  onRecoveryTimeout(const ndn::Interest& interest);

private:  // receive and process data of different kinds of response

  /**
   * @brief  receive the responses of interests
   */
  void
  onData(const ndn::Interest& interest, Data& data);

  void
  onDataValidationFailed(const shared_ptr<const Data>& data, const std::string& reason);

  void
  onDataValidated(const shared_ptr<const Data>& data);

  void
  processSyncData(const Name& name, const char* wireData, size_t len);

  void
  processFetchData(const Name& name, const char* wireData, size_t len);

  void
  processRecoveryData(const Name& name, const char* wireData, size_t len);

  /**
   * @brief  after receive the sync interest response, prepare pipeline to send fetch interest
   * @param  Name       creator name of the action that needs to be fetched
   * @param  uint64_t   the sequnece number of action that needs to be fetched
   */
  void
  prepareFetchForSync(const Name& name, const uint64_t seq);

  /**
   * @brief  after receive the recovery interest response, prepare pipeline to send fetch interest
   * @param  Name       creator name of the action that needs to be fetched
   * @param  uint64_t   the sequnece number of action that needs to be fetched
   */
  void
  prepareFetchForRecovery(const Name& name, const uint64_t seq);

private:  // apply actions and fetch the data

  /**
   * @brief  control the pipeline of fetching actions
   */
  void
  actionControl(const ActionEntry& action);

  /**
   * @brief  apply the action received in local repo
   *         either send the interest to fetch the data, or delete the data
   */
  void
  applyAction(const ActionEntry& action);

  void
  sendNormalInterest(const Name& name);

  void
  onFetchData(const Interest& interest, ndn::Data& data);

  void
  onFetchDataValidated(const shared_ptr<const Data>& data);

  void
  onDataTimeout(const Interest& interest);

private:
  /**
   * @brief Will be thrown when data cannot be properly decoded to SyncStateMsg
   */
  struct SyncStateMsgDecodingFailure : virtual boost::exception, virtual std::exception { };

  /**
   * @brief Will be thrown when obtaining digest from interest
   */
  struct DigestCalculationError : virtual boost::exception, virtual std::exception{ };

private:
  Name m_syncPrefix;    // /ndn/broadcast/
  Name m_outstandingInterestName; //ndn/broadcast/ + type
  Name m_creatorName;
  uint64_t m_seq;       // own action sequence number
  bool m_isSynchronized;
  bool m_isRunning;

  Face& m_face;
  KeyChain& m_keyChain;
  Scheduler m_scheduler;
  ValidatorConfig& m_validator;
  RepoStorage& m_storageHandle;

  std::list<std::pair<ndn::ConstBufferPtr, ActionEntry> > m_actionList;

  //  save the information of local generated actions to provide version number for same actions
  //  currently version number has no use, it can be further implemented to avoid generating
  //  same action continuely
  std::map<std::pair<Name, Action>, uint64_t> m_seqIndex;

  // represent the status of actions with certain creator name
  std::map<Name, pipelineEntrySeq> m_nodeSeq;

  // save actions out of order, name is the creatorName, used by the fething action pipeline
  std::map<Name, std::list<ActionEntry> > m_pendingActionList;

  // record retry times of each action, name is /creatorName/seq
  std::map<Name, int> m_retryTable;

  ndn::EventId m_reexpressingInterestId;
  ndn::EventId m_reexpressingRecoveryInterestId;
  ndn::EventId m_delayedInterestProcessingId;
  ndn::EventId m_synchronizedId;
  SyncTree m_syncTree;
  uint32_t m_recoveryRetransmissionInterval; // milliseconds
  boost::mt19937 m_randomGenerator;
  boost::variate_generator<boost::mt19937&, boost::uniform_int<> > m_rangeUniformRandom;
  boost::variate_generator<boost::mt19937&, boost::uniform_int<> > m_reexpressionJitter;
  InterestTable m_syncInterestTable;
  Msg m_snapshot;
  uint64_t m_snapshotNo;
  std::list<std::pair<Name, uint64_t> > m_snapshotList;
};

//
} // namespace repo

#endif // REPO_SYNC_REPO_SYNC_HPP
