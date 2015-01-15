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

#include "../src/common.hpp"
#include "config.hpp"
#include <string>
#include <sqlite3.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/info_parser.hpp>

namespace repo {

using namespace ndn::time;

void
printUsage(const char* programName)
{

  std::cout
    << "Usage:\n"
    << "  " << programName << " [-c <path/to/repo-ng.conf>] [-h]\n"
    << "\n"
    << "List all the creators and their sequence numbers in Repo Sync Tree Database. "
    << "\n"
    << "Options:\n"
    << "  -h: show help message\n"
    << "  -c: set config file path\n"
    << std::endl;
  ;
}

class RepoEnumerator
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
  RepoEnumerator(const std::string& configFile, bool path);

  uint64_t
  enumerate();

private:
  void
  readConfig(const std::string& configFile);

private:
  bool isPath;
  sqlite3* m_db;
  std::string m_dbPath;
};

RepoEnumerator::RepoEnumerator(const std::string& configFile, bool path)
  : isPath(path)
{
  if (isPath) {
    m_dbPath = configFile;
    m_dbPath += "/ndn_repo_sync.db";
  }
  else {
    readConfig(configFile);
  }
  char* errMsg = 0;
  int rc = sqlite3_open_v2(m_dbPath.c_str(), &m_db,
                           SQLITE_OPEN_READONLY,
   #ifdef DISABLE_SQLITE3_FS_LOCKING
                            "unix-dotfile"
   #else
                            0
   #endif
                          );
  if (rc != SQLITE_OK) {
    throw Error("Database file open failure");
  }
  sqlite3_exec(m_db, "PRAGMA synchronous = OFF", 0, 0, &errMsg);
  sqlite3_exec(m_db, "PRAGMA journal_mode = WAL", 0, 0, &errMsg);
}

void
RepoEnumerator::readConfig(const std::string& configFile)
{
  if (configFile.empty()) {
    throw Error("Invalid configuration file name");
  }

  std::ifstream fin(configFile.c_str());
  if (!fin.is_open())
    throw Error("failed to open configuration file '" + configFile + "'");

  using namespace boost::property_tree;
  ptree propertyTree;
  try {
    read_info(fin, propertyTree);
  }
  catch (ptree_error& e) {
    throw Error("failed to read configuration file '" + configFile + "'");
  }
  ptree repoConf = propertyTree.get_child("repo");
  m_dbPath = repoConf.get<std::string>("storage.path");
  m_dbPath += "/ndn_repo_sync.db";
}

uint64_t
RepoEnumerator::enumerate()
{
  sqlite3_stmt* m_stmt = 0;
  int rc = SQLITE_DONE;
  string sql = string("SELECT name, seq FROM NDN_REPO_SYNC;");
  rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &m_stmt, 0);
  if (rc != SQLITE_OK)
    throw Error("Initiation Read Entries from Database Prepare error");
  uint64_t entryNumber = 0;
  while (true) {
    rc = sqlite3_step(m_stmt);
    if (rc == SQLITE_ROW) {
      Name name;
      name.wireDecode(Block(sqlite3_column_blob(m_stmt, 0),
                            sqlite3_column_bytes(m_stmt, 0)));
      uint64_t seq = sqlite3_column_int64(m_stmt, 1);
      try {
          std::cout << "creator name = "<<name<<" seqNo = "<<seq << std::endl;
      }
      catch (...){
        sqlite3_finalize(m_stmt);
        throw;
      }
      entryNumber++;
    }
    else if (rc == SQLITE_DONE) {
      sqlite3_finalize(m_stmt);
      break;
    }
    else {
      sqlite3_finalize(m_stmt);
      throw Error("Initiation Read Entries error");
    }
  }
  return entryNumber;
}

int
main(int argc, char** argv)
{
  string configPath = "unittestdb_synctree";
  bool isPath = false;
  int opt;
  while ((opt = getopt(argc, argv, "hc:p")) != -1) {
    switch (opt) {
    case 'h':
      printUsage(argv[0]);
      return 0;
    case 'c':
      configPath = string(optarg);
      break;
    case 'p':
      isPath = true;
      break;
    default:
      break;
    }
  }

  RepoEnumerator instance(configPath, isPath);
  uint64_t count = instance.enumerate();
  std::cerr << "Total number of data = " << count << std::endl;
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
