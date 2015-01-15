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


#include "tree-sqlite.hpp"
#include <boost/filesystem.hpp>
#include <istream>

namespace repo {

TreeSqlite::TreeSqlite(const string& dbPath)
  : m_size(0)
{
  if (dbPath.empty()) {
    std::cerr << "Create db file in local location [" << dbPath << "]. " << std::endl
              << "You can assign the path using -d option" << std::endl;
    m_dbPath = string("NDN_REPO_SYNC.db");
  }
  else {
    boost::filesystem::path fsPath(dbPath);
    boost::filesystem::file_status fsPathStatus = boost::filesystem::status(fsPath);
    if (!boost::filesystem::is_directory(fsPathStatus)) {
      if (!boost::filesystem::create_directory(boost::filesystem::path(fsPath))) {
        throw Error("Folder '" + dbPath + "' does not exists and cannot be created");
      }
    }

    m_dbPath = dbPath + "/ndn_repo_sync.db";
  }
  initializeSyncTree();
}

void
TreeSqlite::initializeSyncTree()
{
  char* errMsg = 0;

  int rc = sqlite3_open_v2(m_dbPath.c_str(), &m_db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
#ifdef DISABLE_SQLITE3_FS_LOCKING
                           "unix-dotfile"
#else
                           0
#endif
                           );

  if (rc == SQLITE_OK) {
    sqlite3_exec(m_db, "CREATE TABLE NDN_REPO_SYNC ("
                      "name BLOB PRIMARY KEY, "
                      "seq INTEGER);\n "
                 , 0, 0, &errMsg);
    // Ignore errors (when database already exists, errors are expected)
  }
  else {
    std::cerr << "Database file open failure rc:" << rc << std::endl;
    throw Error("Database file open failure");
  }
  sqlite3_exec(m_db, "PRAGMA synchronous = OFF", 0, 0, &errMsg);
  sqlite3_exec(m_db, "PRAGMA journal_mode = WAL", 0, 0, &errMsg);
}

TreeSqlite::~TreeSqlite()
{
  sqlite3_close(m_db);
}

void
TreeSqlite::fullEnumerate(const ndn::function
                             <void(const TreeStorage::ItemMeta)>& f)
{
  sqlite3_stmt* m_stmt = 0;
  int rc = SQLITE_DONE;
  string sql = string("SELECT name, seq FROM NDN_REPO_SYNC;");
  rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &m_stmt, 0);
  if (rc != SQLITE_OK)
    throw Error("Initiation Read Entries from RepoSync Database Prepare error");
  int entryNumber = 0;
  while (true) {
    rc = sqlite3_step(m_stmt);
    if (rc == SQLITE_ROW) {

      ItemMeta item;
      item.creatorName.wireDecode(Block(sqlite3_column_blob(m_stmt, 0),
                                     sqlite3_column_bytes(m_stmt, 0)));
      item.seq = sqlite3_column_int(m_stmt, 1);
      try {
        f(item);
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
      std::cerr << "Initiation Read Nodes rc:" << rc << std::endl;
      sqlite3_finalize(m_stmt);
      throw Error("Initiation Read Nodes error");
    }
  }
  m_size = entryNumber;
}

void
TreeSqlite::insert(const Name& creator, const uint64_t seq)
{
  int rc = 0;

  sqlite3_stmt* insertStmt = 0;

  string insertSql = string("INSERT INTO NDN_REPO_SYNC (name, seq) "
                            "VALUES (?, ?)");

  if (sqlite3_prepare_v2(m_db, insertSql.c_str(), -1, &insertStmt, 0) != SQLITE_OK) {
    sqlite3_finalize(insertStmt);
    std::cerr << "insert sql not prepared" << std::endl;
  }
  //Insert
  if (sqlite3_bind_blob(insertStmt, 1,
                        creator.wireEncode().wire(),
                        creator.wireEncode().size(), 0) == SQLITE_OK &&
      sqlite3_bind_int64(insertStmt, 2, seq) == SQLITE_OK) {
    rc = sqlite3_step(insertStmt);
    if (rc == SQLITE_CONSTRAINT) {
      std::cerr << "Insert  failed" << std::endl;
      sqlite3_finalize(insertStmt);
      throw Error("Insert failed");
    }
    sqlite3_reset(insertStmt);
    m_size++;
  }
  else {
    throw Error("Some error with insert");
  }
  sqlite3_finalize(insertStmt);
}

void
TreeSqlite::update(const Name& creator, const uint64_t seq)
{
  int rc = 0;
  string updateSql2 = string("UPDATE NDN_REPO_SYNC SET seq = ? WHERE name = ?;");
  //std::cerr << "update" << std::endl;
  sqlite3_stmt* update2Stmt = 0;
  rc = sqlite3_prepare_v2(m_db, updateSql2.c_str(), -1, &update2Stmt, 0);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(update2Stmt);
    std::cerr << "update sql2 not prepared rc : " << rc << std::endl;
    throw Error("update sql2 not prepared");
  }
  if (sqlite3_bind_int64(update2Stmt, 1, seq) == SQLITE_OK &&
      sqlite3_bind_blob(update2Stmt, 2,
                        creator.wireEncode().wire(),
                        creator.wireEncode().size(), 0) == SQLITE_OK) {
    rc = sqlite3_step(update2Stmt);
    sqlite3_finalize(update2Stmt);
    if (rc != SQLITE_DONE) {
      throw Error("Update Node Failed");
    }
  }
  int changeCount = sqlite3_changes(m_db);
  //std::cerr << "changeCount: " << changeCount << std::endl;
  if (changeCount <= 0) {
    throw Error("Update Node Failed");
  }
}

bool
TreeSqlite::erase(const Name& creator)
{
  sqlite3_stmt* deleteStmt = 0;

  string deleteSql = string("DELETE from NDN_REPO_SYNC where name = ?;");

  if (sqlite3_prepare_v2(m_db, deleteSql.c_str(), -1, &deleteStmt, 0) != SQLITE_OK) {
    sqlite3_finalize(deleteStmt);
    std::cerr << "delete statement prepared failed" << std::endl;
    throw Error("delete statement prepared failed");
  }

  if (sqlite3_bind_blob(deleteStmt, 1,
                        creator.wireEncode().wire(),
                        creator.wireEncode().size(), 0) == SQLITE_OK) {
    int rc = sqlite3_step(deleteStmt);
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
      std::cerr << " node delete error rc:" << rc << std::endl;
      sqlite3_finalize(deleteStmt);
      throw Error(" node delete error");
    }
    if (sqlite3_changes(m_db) != 1)
      return false;
    m_size--;
  }
  else {
    std::cerr << "delete bind error" << std::endl;
    sqlite3_finalize(deleteStmt);
    throw Error("delete bind error");
  }
  sqlite3_finalize(deleteStmt);
  return true;
}


uint64_t
TreeSqlite::read(const Name& creator)
{
  sqlite3_stmt* queryStmt = 0;
  string sql = string("SELECT * FROM NDN_REPO_SYNC WHERE name = ? ;");
  int rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &queryStmt, 0);
  if (rc == SQLITE_OK) {
    if (sqlite3_bind_blob(queryStmt, 1,
                          creator.wireEncode().wire(),
                          creator.wireEncode().size(), 0) == SQLITE_OK) {
      rc = sqlite3_step(queryStmt);
      if (rc == SQLITE_ROW) {
        uint64_t seqNo;
        seqNo = sqlite3_column_int64(queryStmt, 1);
        sqlite3_finalize(queryStmt);
        return seqNo;
      }
      else if (rc == SQLITE_DONE) {
        return 0;
      }
      else {
        std::cerr << "Database query failure rc:" << rc << std::endl;
        sqlite3_finalize(queryStmt);
        throw Error("Database query failure");
      }
    }
    else {
      std::cerr << "select bind error" << std::endl;
      sqlite3_finalize(queryStmt);
      throw Error("select bind error");
    }
    sqlite3_finalize(queryStmt);
  }
  else {
    sqlite3_finalize(queryStmt);
    std::cerr << "select statement prepared failed" << std::endl;
    throw Error("select statement prepared failed");
  }
  return 0;
}

uint64_t
TreeSqlite::size()
{
  sqlite3_stmt* queryStmt = 0;
  string sql("SELECT count(*) FROM NDN_REPO_SYNC ");
  int rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &queryStmt, 0);
  if (rc != SQLITE_OK)
    {
      std::cerr << "Database query failure rc:" << rc << std::endl;
      sqlite3_finalize(queryStmt);
      throw Error("Database query failure");
    }

  rc = sqlite3_step(queryStmt);
  if (rc != SQLITE_ROW)
    {
      std::cerr << "Database query failure rc:" << rc << std::endl;
      sqlite3_finalize(queryStmt);
      throw Error("Database query failure");
    }

  uint64_t nDatas = sqlite3_column_int64(queryStmt, 0);
  if (m_size != nDatas) {
    std::cerr << "The size of database is not correct! " << std::endl;
  }
  return nDatas;
}

} //namespace repo
