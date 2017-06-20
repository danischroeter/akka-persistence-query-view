Persistence query view
======================

[![CircleCI Badge](https://circleci.com/gh/ovotech/akka-persistence-query-view.svg?style=shield)](https://circleci.com/gh/ovotech/akka-persistence-query-view)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5d8922799fdc44d48764e8f647ba28dc)](https://www.codacy.com/app/me_62/akka-persistence-query-view?utm_source=github.com&utm_medium=referral&utm_content=ovotech/akka-persistence-query-view&utm_campaign=badger)
[![Download](https://api.bintray.com/packages/ovotech/maven/akka-persistence-query-view/images/download.svg)](https://bintray.com/ovotech/maven/akka-persistence-query-view/_latestVersion)

The `QueryView` is a replacement of the deprecated `PersistentView` in Akka Persistence module.

## Anatomy of a Persistence QueryView
The Persistence query view has three possible state: `WaitingForSnapshot`, `Recovering` and `Live`. 

It always start in `WaitingForSnapshot` state where it is waiting to receive a previously saved snapshot. When the snapshot has been loaded or failed to load, the view switch to the `Recovering` state. 
During the `Recovering` state it will receive all the past events from the journal. When all the existing event from the journal have been consumed, the view will switch to the `Live` state which will keep until the actor stop. 
During the `Live` events the view will consume live events from the journal and external messages.

When the view is in `WaitingForSnapshot` or `Recovering` it will not reply to any messages, but will stash them waiting to switch to the `Live` state where these message will be processed.

## Adding the dependency

Add a dependency to your `build.sbt`:

```
resolvers += Resolver.bintrayRepo("ovotech", "maven")
libraryDependencies += "com.ovoenergy" %% "akka-persistence-query-view" % "<version>"
```

## How to implement
The first step is to define a `Querysupport` trait for your `ReadJournal` plugin. The LevelDb one is included:
```scala
import akka.contrib.persistence.query.QuerySupport
import akka.persistence.QueryView
import akka.persistence.query.{Offset, PersistenceQuery}
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal

trait LevelDbQuerySupport extends QuerySupport { this: QueryView =>

  override type Queries = LeveldbReadJournal
  override def firstOffset: Offset = Offset.sequence(1L)
  override val queries: LeveldbReadJournal =
    PersistenceQuery(context.system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
}
```

It is up to the implementor defining the queries used during the `Recovering` and `Live` states. Generally they will be the same query, with the difference that the recovery one is a finite stream while the live one is infinite. 
Your `Queryview` implemention has to mix in one `QuerySupport` trait as well:

```scala
import akka.stream.scaladsl.Source

case class Person(name: String, age: Int)
case class PersonAdded(person: Person)
case class PersonRemoved(person: Person)

class PersonsQueryView extends QueryView with LevelDbQuerySupport {

  override val snapshotterId: String = "people"

  private var people: Set[Person] = Set.empty

  override def recoveringStream(): Source[AnyRef, _] =
    queries.currentEventsByTag("person", lastOffset)

  override def liveStream(): Source[AnyRef, _] =
    queries.eventsByTag("person", lastOffset)

  override def receive: Receive = {

    case PersonAdded(person) =>
      people = people + person

    case PersonRemoved(person) =>
      people = people - person

  }
}
```

The `WaitingForSnapshot` and `Recovering` states are protected by a timeout, if the view will not be able to rebuild its status within this timeout, it will switch to the `Live` state or crash. This behavior is controlled by the `recovery-timeout-strategy` (TODO) option.

The `QueryView` has an out-of-the-box support for snapshot. It is the same as the deprecated `PersistentView`, in the previous exaple to save a snapshot of the current people:

```scala
import akka.stream.scaladsl.Source

case class Person(name: String, age: Int)
case class PersonAdded(person: Person)
case class PersonRemoved(person: Person)

class PersonsQueryView extends QueryView with LevelDbQuerySupport {

  override val snapshotterId: String = "people"

  private var people: Set[Person] = Set.empty

  override def recoveringStream(): Source[AnyRef, _] =
    queries.currentEventsByTag("person", lastOffset)

  override def liveStream(): Source[AnyRef, _] =
    queries.eventsByTag("person", lastOffset)

  override def receive: Receive = {

    case PersonAdded(person) =>
      people = people + person
      if(noOfEventSinceLastSnapshot() > 100) {
        saveSnapshot(people)
      }

    case PersonRemoved(person) =>
      people = people - person
      if(noOfEventSinceLastSnapshot() > 100) {
        saveSnapshot(people)
      }

  }
}
```

Under the hood it will store also the last consumed offset and the last sequence number for each persistence id already consumed.

The QueryView checks that all received events follow a strict sequence per persistentId. Events read out of order are held in memory and only applied once all previous events have been received.

### Forced Update
Most journals use some sort of polling under the hood to support a live stream for `eventsByTag/eventsByPersistentId` PersistentQueries. (The default cassandra journal uses 3 seconds)
In scenarios when a more up to date state is needed one can issue a forced update which will immediately read from the recoveringStream. (Use `forceUpdate()` or send a ForceUpdate).
The QueryView ensures forcedUpdate is not performed concurrently so forceUpdate is ignored while it has not completed. After forceUpdate is completed `onForceUpdateCompleted()` is called.
For some scenarios it makes sense to retrigger `forceUpdate()` within `onForceUpdateCompleted()` until some condition is met.

## Future developments
 * Add the `recovery-timeout-strategy` option to control what to do when the view does ot recover within a certain amount of time.

## About this README

The code samples in this README file are checked using [tut](https://github.com/tpolecat/tut).

This means that the `README.md` file is generated from `src/main/tut/README.md`. If you want to make any changes to the README, you should:

1. Edit `src/main/tut/README.md`
2. Run `sbt tut` to regenerate `./README.md`
3. Commit both files to git
