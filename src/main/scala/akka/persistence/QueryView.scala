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

package akka.persistence

import akka.actor._
import akka.contrib.persistence.query.{LiveStreamCompletedException, QueryViewSnapshot}
import akka.persistence.SnapshotProtocol.{LoadSnapshotFailed, LoadSnapshotResult}
import akka.persistence.query.{EventEnvelope, EventEnvelope2, Sequence}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

object QueryView {

  val DefaultRecoveryTimeout: Duration = 120.seconds

  val DefaultLoadSnapshotTimeout: Duration = 5.seconds

  private case object StartRecovery

  private case object StartLive

  private case object EventReplayed

  private case object LoadSnapshotTimeout

  private case object RecoveryCompleted

  private case class RecoveryFailed(cause: Throwable)

  private case class LiveStreamFailed(cause: Throwable)

  /**
    * Additionally to being updated by the live stream the QueryView instantly issues a query using the recovery stream to perform a fast forced update
    * (useful in corner cases when the live stream has a high delay/polling interval)
    * While updating a subsequent ForceUpdate is ignored.
    */
  case object ForceUpdate

  private case object StartForceUpdate

  private case object ForceUpdateCompleted

  private case class ForceUpdateFailed(cause: Throwable)

  sealed trait State

  object State {

    case object WaitingForSnapshot extends State

    case object Recovering extends State

    case object Live extends State

  }
}

trait EventStreamOffsetTyped {

  /**
    * Type of offset used for position in the event stream
    */
  //todo replace with 'OT <: Offset' but not possible as long as EventEnvelope (not EventEnvelope2) is used which is bound to Sequence
  type OT = Sequence
}

abstract class QueryView
    extends Actor
    with Snapshotter
    with EventStreamOffsetTyped
    with Stash
    with StashFactory
    with ActorLogging {

  import QueryView._
  import context._

  // Status variables

  def firstOffset: OT

  private[this] var _lastOffset: OT = firstOffset
  private[this] var _sequenceNrByPersistenceId: Map[String, Long] = Map.empty
  private[this] var _futureEventsByPersistenceId: Map[String, List[EventEnvelope2]] = Map.empty
  private[this] var lastSnapshotSequenceNr: Long = 0L
  private[this] var _noOfEventsSinceLastSnapshot: Long = 0L
  private[this] var _currentEvent: Option[(String,Long)] = None
  private[this] var currentState: State = State.WaitingForSnapshot
  private[this] var loadSnapshotTimer: Option[Cancellable] = None
  private[this] var savingSnapshot: Boolean = false
  private[this] var forcedUpdateInProgress: Boolean = false

  private val persistence = Persistence(context.system)
  override private[persistence] val snapshotStore: ActorRef = persistence.snapshotStoreFor(snapshotPluginId)
  private implicit val materializer = ActorMaterializer()(context)

  /**
    * This stash will contain the messages received during the recovery phase.
    */
  private val recoveringStash = createStash()

  /**
    * It is the persistenceId linked to this view. It should be unique.
    */
  override def snapshotterId: String

  /**
    * Can be accessed while processing an event (in #receive) to get its persistenceId/sequenceNr otherwise fails with java.util.NoSuchElementException
    * @return
    */
  def currentEvent: (String,Long) = _currentEvent.get

  /**
    * Configuration id of the snapshot plugin servicing this persistent actor or view.
    * When empty, looks in `akka.persistence.snapshot-store.plugin` to find configuration entry path.
    * When configured, uses `snapshotPluginId` as absolute path to the snapshot store configuration entry.
    * Configuration entry must contain few required fields, such as `class`. See akka-persistence jar
    * `src/main/resources/reference.conf`.
    */
  def snapshotPluginId: String = ""

  /**
    * The amount of time this actor must wait until giving up waiting for the recovery process. A undefined duration
    * causes the actor to wait indefinitely. If the recovery fails because of a timeout, this actor will crash.
    *
    * TODO Tune by a flag to indicate we want the actor to switch live if the recovery timeout.
    */
  def recoveryTimeout: Duration = DefaultRecoveryTimeout

  /**
    * The amount of time this actor must wait until giving up waiting for a snapshot loading. A undefined duration
    * causes the actor to wait indefinitely. The timeout does not cause this actor to crash, it is a recoverable error.
    */
  def loadSnapshotTimeout: Duration = DefaultLoadSnapshotTimeout

  /**
    * It is the source od EventEnvelope used to recover the view status. It MUST be finite stream.
    *
    * It is declared as AnyRef to be able to return [[EventEnvelope]] or [[EventEnvelope2]].
    */
  def recoveringStream(sequenceNrByPersistenceId: Map[String, Long], lastOffset: OT): Source[AnyRef, _]

  /**
    * It is the source od EventEnvelope used to receive live events, it MUST be a infinite stream (eg: It should never
    * complete)
    *
    * It is declared as AnyRef to be able to return [[EventEnvelope]] or [[EventEnvelope2]].
    */
  def liveStream(sequenceNrByPersistenceId: Map[String, Long], lastOffset: OT): Source[AnyRef, _]

  /**
    * It is an hook called before the actor switch to live mode. It is synchronous (it can change the actor status).
    * It can be useful to fetch additional data from other actor/services before starting receiving messages.
    */
  def preLive(): Unit = {}

  /**
    * @see [[ForceUpdate]]
    */
  def forceUpdate(): Unit = startForceUpdate()

  /**
    * Is called when the stream of a forceUpdate has completed
    */
  def onForceUpdateCompleted() = {}

  // Status accessors

  /**
    * Return if this actor is waiting for receiving the snapshot from the snapshot-store.
    */
  final def isWaitingForSnapshot: Boolean = currentState == State.WaitingForSnapshot

  /**
    * Return if this actor is in recovery phase. Useful to the implementor to apply different behavior when a message
    * came from the journal or from another actor.
    */
  final def isRecovering: Boolean = currentState == State.Recovering

  /**
    * Return if this actor is in live phase. Useful to the implementor to apply different behavior when a message
    * came from the journal or from another actor.
    */
  final def isLive: Boolean = currentState == State.Live

  /**
    * Return the last replayed message offset from the journal.
    */
  final def lastOffset: OT = Option(_lastOffset).getOrElse(firstOffset)

  /**
    * The current sequenceNr of given persistenceId
    *
    * @param persistenceId
    * @return
    */
  final def lastSequenceNrFor(persistenceId: String): Long = _sequenceNrByPersistenceId.getOrElse(persistenceId, 0)

  /**
    * Return the number of processed events since last snapshot has been taken.
    */
  final def noOfEventSinceLastSnapshot(): Long = _noOfEventsSinceLastSnapshot

  /**
    * Return the next sequence nr to apply to the next snapshot.
    */
  override final def snapshotSequenceNr: Long = lastSnapshotSequenceNr + 1

  // Behavior

  override protected[akka] def aroundPreStart(): Unit = {
    loadSnapshot()
    super.aroundPreStart()
  }

  override protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    loadSnapshot()
    super.aroundPreRestart(reason, message)
  }

  private def loadSnapshot(): Unit = {
    // If the `loadSnapshotTimeout` is finite, it makes sure the Actor will not get stuck in 'waitingForSnapshot' state.
    loadSnapshotTimer = loadSnapshotTimeout match {
      case timeout: FiniteDuration =>
        Some(
          context.system.scheduler.scheduleOnce(
            timeout,
            self,
            LoadSnapshotTimeout
          )
        )
      case _ =>
        None
    }
    currentState = State.WaitingForSnapshot
    loadSnapshot(snapshotterId, SnapshotSelectionCriteria.Latest, Long.MaxValue)
  }

  override protected[akka] def aroundPostRestart(reason: Throwable): Unit = {
    cancelSnapshotTimer()
    super.aroundPostRestart(reason)
  }

  private def cancelSnapshotTimer(): Unit = loadSnapshotTimer.foreach(_.cancel())

  override protected[akka] def aroundPostStop(): Unit = {
    cancelSnapshotTimer()
    materializer.shutdown()
    super.aroundPostStop()
  }

  override protected[akka] def aroundReceive(behaviour: Receive, msg: Any): Unit = {
    log.debug(s"Query view in state [$currentState] received message: [$msg]")
    if (isWaitingForSnapshot) {
      waitingForSnapshot(behaviour, msg)
    } else if (isRecovering) {
      recovering(behaviour, msg)
    } else {
      assert(isLive)
      live(behaviour, msg)
    }
  }

  private def live(behaviour: Receive, msg: Any) =
    msg match {
      case StartLive =>
        sender() ! EventReplayed

      case e: EventEnvelope2 ⇒
        processEvent(behaviour, e)
        sender() ! EventReplayed

      case EventEnvelope(offset, persistenceId, sequenceNr, event) ⇒
        processEvent(behaviour, EventEnvelope2(Sequence(offset),persistenceId,sequenceNr,event))
        sender() ! EventReplayed

      case LiveStreamFailed(ex) ⇒
        log.error(ex, s"Live stream failed, it is a fatal error")
        // We have to crash the actor
        throw ex

      case ForceUpdate ⇒
        startForceUpdate()

      case StartForceUpdate ⇒
        log.debug("update stream started")
        sender() ! EventReplayed

      case ForceUpdateCompleted ⇒
        forcedUpdateInProgress = false
        onForceUpdateCompleted()

      case ForceUpdateFailed(f) ⇒
        log.error(f, "forceupdate failed")
        forcedUpdateInProgress = false

      case msg @ SaveSnapshotSuccess(metadata) ⇒
        snapshotSaved(metadata)
        super.aroundReceive(behaviour, msg)

      case msg @ SaveSnapshotFailure(metadata, error) ⇒
        snapshotSavingFailed(metadata, error)
        super.aroundReceive(behaviour, msg)

      case _ ⇒
        super.aroundReceive(behaviour, msg)
    }

  private def recovering(behaviour: Receive, msg: Any) =
    msg match {
      case StartRecovery =>
        sender() ! EventReplayed

      case e: EventEnvelope2 ⇒
        processEvent(behaviour, e)
        sender() ! EventReplayed

      case EventEnvelope(offset, persistenceId, sequenceNr, event) ⇒
        processEvent(behaviour, EventEnvelope2(Sequence(offset),persistenceId,sequenceNr,event))
        sender() ! EventReplayed

      case QueryView.RecoveryCompleted ⇒
        log.info("Recovery completed")
        startLive()

      case RecoveryFailed(ex) ⇒
        // TODO if it is a Timeout decide if switch to live or crash
        log.error(ex, "Error recovering")
        throw ex

      case msg @ SaveSnapshotSuccess(metadata) ⇒
        snapshotSaved(metadata)
        super.aroundReceive(behaviour, msg)

      case msg @ SaveSnapshotFailure(metadata, error) ⇒
        snapshotSavingFailed(metadata, error)
        super.aroundReceive(behaviour, msg)

      case LiveStreamFailed(ex) =>
        log.error(ex, s"Live stream failed while recovering, ignoring...")

      case LoadSnapshotTimeout =>
        log.error("Unexpected load snapshot timeout while recovering.")

      case LoadSnapshotFailed(ex) =>
        log.error(ex, "Unexpected snapshot failed error while recovering.")

      case other =>
        log.debug(s"Stashing while recovering: [$other]")
        recoveringStash.stash()
    }

  private def processEvent(behaviour: Receive, ee: EventEnvelope2): Unit = {
    val expectedNextSeqForPersistenceId = _sequenceNrByPersistenceId.getOrElse(ee.persistenceId, 0L) + 1
    if (ee.sequenceNr > expectedNextSeqForPersistenceId) {
      log.debug("postpone processing of future event with seq {} for persistenceId {} with expectedSeq {}", ee.sequenceNr, ee.persistenceId, expectedNextSeqForPersistenceId)
      val newFutureEventsForPid = ee :: _futureEventsByPersistenceId.getOrElse(ee.persistenceId, List.empty)
      _futureEventsByPersistenceId = _futureEventsByPersistenceId + (ee.persistenceId -> newFutureEventsForPid)
    }
    else if (ee.sequenceNr == expectedNextSeqForPersistenceId) {
      _lastOffset = ee.offset.asInstanceOf[OT]
      _sequenceNrByPersistenceId = _sequenceNrByPersistenceId + (ee.persistenceId -> ee.sequenceNr)
      _noOfEventsSinceLastSnapshot = _noOfEventsSinceLastSnapshot + 1

      _currentEvent = Some((ee.persistenceId,ee.sequenceNr))
      super.aroundReceive(behaviour, ee.event)
      _currentEvent = None

      _futureEventsByPersistenceId.get(ee.persistenceId).map(fes => fes.sortBy(_.sequenceNr)).foreach(fes => {
        _futureEventsByPersistenceId = _futureEventsByPersistenceId + (ee.persistenceId -> fes.tail)
        fes.headOption.foreach(fe => processEvent(behaviour, fe))
      })
    }
    else {
      log.debug("filter already processed event for sequenceNr {} event {}", ee.sequenceNr, ee.event)
    }
  }

  private def waitingForSnapshot(behaviour: Receive, msg: Any) =
    msg match {
      case LoadSnapshotResult(Some(SelectedSnapshot(metadata, status: QueryViewSnapshot[_])), _) ⇒
        val offer = SnapshotOffer(metadata, status.data)
        if (behaviour.isDefinedAt(offer)) {
          super.aroundReceive(behaviour, offer)
          _lastOffset = status.maxOffset.asInstanceOf[OT]
          _sequenceNrByPersistenceId = status.sequenceNrs
          lastSnapshotSequenceNr = metadata.sequenceNr
        }
        startRecovery()

      case LoadSnapshotResult(None, _) =>
        startRecovery()

      case LoadSnapshotTimeout =>
        // It is recoverable so we don't need to crash the actor
        log.error(s"Timeout loading the snapshot after [$loadSnapshotTimeout]")
        startRecovery()

      case LoadSnapshotFailed(ex) =>
        log.error(ex, "Error while loading the snapshot.")
        startRecovery()

      case LiveStreamFailed(ex) =>
        log.error(ex, s"Live stream failed while waiting for snapshot, ignoring...")

      case other =>
        log.debug(s"Stashing while waiting for snapshot: [$other]")
        recoveringStash.stash()
    }

  private def startRecovery(): Unit = {

    loadSnapshotTimer.foreach(_.cancel())
    currentState = State.Recovering

    val stream = recoveryTimeout match {
      case t: FiniteDuration => recoveringStream(_sequenceNrByPersistenceId, lastOffset).completionTimeout(t)
      case _ => recoveringStream(_sequenceNrByPersistenceId, lastOffset)
    }

    val recoverySink =
      Sink.actorRefWithAck(self, StartRecovery, EventReplayed, QueryView.RecoveryCompleted, e => RecoveryFailed(e))

    stream.to(recoverySink).run()
  }

  private def startLive(): Unit = {

    preLive()

    currentState = State.Live
    recoveringStash.unstashAll()

    val liveSink =
      Sink.actorRefWithAck(
        self,
        StartLive,
        EventReplayed,
        LiveStreamFailed(new LiveStreamCompletedException),
        e => LiveStreamFailed(e)
      )

    liveStream(_sequenceNrByPersistenceId, lastOffset).to(liveSink).run()
  }

  private def startForceUpdate(): Unit =
    if (forcedUpdateInProgress) {
      log.debug("ignore forceupdate since forceupdate is already in progress")
    } else {
      log.debug("forceupdate for persistentid {} and offset {}", _sequenceNrByPersistenceId, lastOffset)
      val forceUpdateSink =
        Sink.actorRefWithAck(self, StartForceUpdate, EventReplayed, ForceUpdateCompleted, e => ForceUpdateFailed(e))
      recoveringStream(_sequenceNrByPersistenceId, lastOffset).to(forceUpdateSink).run()
    }

  override def saveSnapshot(snapshot: Any): Unit = if (!savingSnapshot) {
    // Decorate the snapshot
    savingSnapshot = true
    super.saveSnapshot(QueryViewSnapshot(snapshot, _lastOffset, _sequenceNrByPersistenceId))
  }

  private def snapshotSaved(metadata: SnapshotMetadata): Unit = {
    savingSnapshot = false
    lastSnapshotSequenceNr = metadata.sequenceNr
    _noOfEventsSinceLastSnapshot = 0L
    log.debug(
      s"Snapshot saved successfully snapshotterId=$snapshotterId lastSnapshotSequenceNr=$lastSnapshotSequenceNr"
    )

  }

  private def snapshotSavingFailed(metadata: SnapshotMetadata, error: Throwable): Unit = {
    savingSnapshot = false
    log.error(error, s"Error saving snapshot snapshotterId=$snapshotterId")
  }
}
