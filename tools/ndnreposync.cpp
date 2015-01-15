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
 * repo-ng, e.g., in COPYING.md file.  if (not, see <http://www.gnu.org/licenses/>.
 */

#include "../src/repo-command-parameter.hpp"
#include "../src/repo-command-response.hpp"

#include <ndn-cxx/face.hpp>
#include <ndn-cxx/security/key-chain.hpp>
#include <ndn-cxx/util/scheduler.hpp>
#include <string>
#include <stdlib.h>
#include <stdint.h>

#include <boost/lexical_cast.hpp>

namespace repo {

using namespace ndn::time;
static const uint64_t DEFAULT_INTEREST_LIFETIME = 4000;
static const uint64_t DEFAULT_CHECK_PERIOD = 1000;

enum CommandType
{
  START,
  CHECK,
  STOP
};

class NdnRepoSync : ndn::noncopyable
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

  NdnRepoSync()
    : status(START)
    , interestLifetime(DEFAULT_INTEREST_LIFETIME)
    , m_scheduler(m_face.getIoService())
    , m_checkPeriod(DEFAULT_CHECK_PERIOD)
  {
  }

  void
  run();

private:

  void
  startSyncCommand();

  void
  onSyncCommandResponse(const ndn::Interest& interest, ndn::Data& data);

  void
  onSyncCommandTimeout(const ndn::Interest& interest);

  void
  stopProcess();

  void
  signData(ndn::Data& data);

  void
  startCheckCommand();

  void
  onCheckCommandTimeout(const ndn::Interest& interest);

  void
  onStopCommandResponse(const ndn::Interest& interest, ndn::Data& data);

  void
  onStopCommandTimeout(const ndn::Interest& interest);

  ndn::Interest
  generateCommandInterest(const ndn::Name& commandPrefix, const std::string& command,
                          const RepoCommandParameter& commandParameter);

public:
  std::string identityForCommand;
  CommandType status;
  ndn::Name repoPrefix;
  ndn::Name creatorName;
  bool hasCreatorName;
  milliseconds interestLifetime;

private:
  ndn::Face m_face;
  ndn::Scheduler m_scheduler;
  milliseconds m_checkPeriod;

  ndn::Name m_dataPrefix;
  ndn::KeyChain m_keyChain;
};

void
NdnRepoSync::run()
{
  creatorName = creatorName;
  startSyncCommand();

  m_face.processEvents();
}

void
NdnRepoSync::startSyncCommand()
{
  RepoCommandParameter parameters;
  if (hasCreatorName)
    parameters.setName(creatorName);

  repoPrefix.append("sync");
  if (status == START) {
    ndn::Interest commandInterest = generateCommandInterest(repoPrefix, "start", parameters);
    m_face.expressInterest(commandInterest,
                           ndn::bind(&NdnRepoSync::onSyncCommandResponse, this, _1, _2),
                           ndn::bind(&NdnRepoSync::onSyncCommandTimeout, this, _1));
  }
  else if (status == STOP){
    ndn::Interest commandInterest = generateCommandInterest(repoPrefix, "stop", parameters);
    m_face.expressInterest(commandInterest,
                           ndn::bind(&NdnRepoSync::onSyncCommandResponse, this, _1, _2),
                           ndn::bind(&NdnRepoSync::onSyncCommandTimeout, this, _1));
  }
  else if (status == CHECK){
    ndn::Interest commandInterest = generateCommandInterest(repoPrefix, "check", parameters);
    m_face.expressInterest(commandInterest,
                           ndn::bind(&NdnRepoSync::onSyncCommandResponse, this, _1, _2),
                           ndn::bind(&NdnRepoSync::onSyncCommandTimeout, this, _1));
  }

}

void
NdnRepoSync::onSyncCommandResponse(const ndn::Interest& interest, ndn::Data& data)
{
  RepoCommandResponse response(data.getContent().blockFromValue());
  int statusCode = response.getStatusCode();
  if (statusCode >= 400) {
    throw Error("Sync command failed with code " +
                boost::lexical_cast<std::string>(statusCode));
  }
  else if (statusCode == 300) {
    std::cerr << "Syncing prefix is stopped!" <<std::endl;
    m_face.getIoService().stop();
    return;
  }
  else if (statusCode == 200) {
    std::cerr << "Syncing prefix is running!" <<std::endl;
    m_scheduler.scheduleEvent(m_checkPeriod,
                              ndn::bind(&NdnRepoSync::startCheckCommand, this));
    return;
  }
  else if (statusCode == 100) {
    std::cerr << "Syncing prefix starts!" <<std::endl;
    m_scheduler.scheduleEvent(m_checkPeriod,
                              ndn::bind(&NdnRepoSync::startCheckCommand, this));
    return;
  }
  else {
    throw Error("Unrecognized Status Code " +
                boost::lexical_cast<std::string>(statusCode));
  }
}

void
NdnRepoSync::onSyncCommandTimeout(const ndn::Interest& interest)
{
  throw Error("command response timeout");
}

void
NdnRepoSync::stopProcess()
{
  m_face.getIoService().stop();
}

void
NdnRepoSync::startCheckCommand()
{
  repoPrefix.append("sync");
  ndn::Interest checkInterest = generateCommandInterest(repoPrefix, "check",
                                                        RepoCommandParameter()
                                                          .setName(creatorName));
  m_face.expressInterest(checkInterest,
                         ndn::bind(&NdnRepoSync::onSyncCommandResponse, this, _1, _2),
                         ndn::bind(&NdnRepoSync::onCheckCommandTimeout, this, _1));
}

void
NdnRepoSync::onCheckCommandTimeout(const ndn::Interest& interest)
{
  throw Error("check response timeout");
}

void
NdnRepoSync::onStopCommandResponse(const ndn::Interest& interest, ndn::Data& data)
{
  RepoCommandResponse response(data.getContent().blockFromValue());
  int statusCode = response.getStatusCode();
  if (statusCode != 300) {
    throw Error("Sync stop command failed with code: " +
                boost::lexical_cast<std::string>(statusCode));
  }
  else {
    std::cerr << "Status code is 300. Syncing prefix is stopped successfully!" << std::endl;
    m_face.getIoService().stop();
    return;
  }
}

void
NdnRepoSync::onStopCommandTimeout(const ndn::Interest& interest)
{
  throw Error("stop response timeout");
}

ndn::Interest
NdnRepoSync::generateCommandInterest(const ndn::Name& commandPrefix, const std::string& command,
                                      const RepoCommandParameter& commandParameter)
{
  ndn::Interest interest(ndn::Name(commandPrefix)
                         .append(command)
                         .append(commandParameter.wireEncode()));
  interest.setInterestLifetime(interestLifetime);

  if (identityForCommand.empty())
    m_keyChain.sign(interest);
  else {
    m_keyChain.signByIdentity(interest, ndn::Name(identityForCommand));
  }
  return interest;
}

static void
usage()
{
  fprintf(stderr,
          "  NdnRepoSync [-s stop] [-c check] [-l lifetime] [-n Creator Name]repo-prefix\n"
          "  Start/Stop/Check RepoSync process.\n"
          "  -s: stop the whole process (the default is start the process)\n"
          "  -c: check the process\n"
          "  -n: check the process\n"
          "  repo-prefix: repo command prefix\n"
          );
  exit(1);
}

int
main(int argc, char** argv)
{
  NdnRepoSync app;
  int opt;
  while ((opt = getopt(argc, argv, "scl:n:")) != -1) {
    switch (opt) {
    case 's':
      app.status = STOP;
      break;
    case 'c':
      app.status = CHECK;
      break;
    case 'l':
      try {
        app.interestLifetime = milliseconds(boost::lexical_cast<uint64_t>(optarg));
      }
      catch (boost::bad_lexical_cast&) {
        std::cerr << "-l option should be an integer.";
        return 1;
      }
      break;
    case 'n':
      app.creatorName = Name(std::string(optarg));
      app.hasCreatorName = true;
      break;
    case 'h':
      usage();
      break;
    default:
      break;
    }
  }

  argc -= optind;
  argv += optind;

  if (argc != 1)
    usage();
  app.repoPrefix = Name(argv[0]);
  app.run();

  return 0;
}

} // namespace repo

int
main(int argc, char** argv)
{
  try {
    return repo::main(argc, argv);
  }
  catch (std::exception& e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
    return 2;
  }
}
