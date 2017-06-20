/*
 * Copyright 2016 OVO Energy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.contrib.persistence.query

import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.{PersistenceQuery, Sequence}
import akka.persistence.{EventStreamOffsetTyped, QueryView}

trait QuerySupport extends EventStreamOffsetTyped {

  type Queries

  def queries: Queries

  def firstOffset: OT
}

trait LevelDbQuerySupport extends QuerySupport {
  this: QueryView =>

  override type Queries = LeveldbReadJournal
  override def firstOffset: OT = Sequence(0L)
  override val queries: LeveldbReadJournal =
    PersistenceQuery(context.system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
}
