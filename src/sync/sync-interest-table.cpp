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

#include "sync-interest-table.hpp"

namespace repo
{

InterestTable::InterestTable(boost::asio::io_service& io,
                             ndn::time::system_clock::Duration lifetime,
                             ndn::time::system_clock::Duration cleanPeriod/*=4sec*/)
  : m_scheduler(io)
  , m_entryLifetime(lifetime)
  , m_cleanPeriod(cleanPeriod)
{
  m_scheduler.scheduleEvent(m_cleanPeriod,
                            ndn::bind(&InterestTable::expireInterests, this));
}

bool
InterestTable::insert(ndn::ConstBufferPtr digest, const Name& name, bool isKnown/*=false*/)
{
  bool doesExist = false;

  InterestContainer::index<hashed>::type::iterator it = m_table.get<hashed>().find(digest);
  if (it != m_table.get<hashed>().end()) {
      doesExist = true;
      m_table.erase(it);
  }
  m_table.insert(boost::make_shared<PendingInterest>(digest, name, isKnown));

  return doesExist;
}

void
InterestTable::remove(ndn::ConstBufferPtr digest)
{
  InterestContainer::index<hashed>::type::iterator it = m_table.get<hashed>().find (digest);
  if (it != m_table.get<hashed>().end())
    m_table.get<hashed>().erase (digest);
}

ConstPendingInterestPtr
InterestTable::pop()
{
  if (m_table.size() == 0)
    throw Error("InterestTable is empty");

  ConstPendingInterestPtr interest = *m_table.begin();
  m_table.erase(m_table.begin());

  return interest;
}

ConstPendingInterestPtr
InterestTable::begin()
{
  if (m_table.size() == 0)
    throw Error("InterestTable is empty");

  ConstPendingInterestPtr interest = *m_table.begin();
  return interest;
}

size_t
InterestTable::size() const
{
  return m_table.size();
}

void
InterestTable::reset()
{
  m_table.clear();
}

void
InterestTable::expireInterests()
{
  ndn::time::system_clock::TimePoint expireTime = ndn::time::system_clock::now() - m_entryLifetime;

  while (m_table.size() > 0) {
    InterestContainer::index<timed>::type::iterator it = m_table.get<timed>().begin();

    if ((*it)->m_time <= expireTime)
      m_table.get<timed>().erase(it);
    else
      break;
  }

  m_scheduler.scheduleEvent(m_cleanPeriod,
                            ndn::bind(&InterestTable::expireInterests, this));
}


} // namespace repo
