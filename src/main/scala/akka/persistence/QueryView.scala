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
import akka.contrib.persistence.query.{LiveStreamCompletedException, QuerySupport, QueryViewSnapshot}
import akka.persistence.SnapshotProtocol.LoadSnapshotResult
import akka.persistence.query.{EventEnvelope, EventEnvelope2, Offset, Sequence}
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

  private case class LoadSnapshotFailed(cause: Throwable)

  private case object RecoveryCompleted

  private case class RecoveryFailed(cause: Throwable)

  private case class LiveStreamFailed(cause: Throwable)

  sealed trait State

  object State {

    case object WaitingForSnapshot extends State

    case object Recovering extends State

    case object Live extends State

  }

  private object PersistenceIdAndSequenceNr {

    def unapply(arg: AnyRef): Option[(String, Long)] = arg match {
      case EventEnvelope2(_, persistenceId, sequenceNr, _) => Some(persistenceId -> sequenceNr)
      case EventEnvelope(_, persistenceId, sequenceNr, _) => Some(persistenceId -> sequenceNr)
      case _ => None
    }
  }

  implicit class RichSource[T, M](s: Source[T, M]) {
    def filterAlreadyReceived(nextSequenceNrsByPersistenceId: Map[String, Long]): Source[T, M] =
      s.filter {
        // This will avoid duplicate events due to low precision offset.
        case PersistenceIdAndSequenceNr(pId, sNr) =>
          nextSequenceNrsByPersistenceId.get(pId).forall(sNr >= _)
        case _ => false // We don't want to make pass anything else
      }
  }

}

abstract class QueryView
    extends Actor
    with Snapshotter
    with QuerySupport
    with Stash
    with StashFactory
    with ActorLogging {

  import QueryView._
  import context._

  // Status variables

  private[this] var _lastOffset: Offset = firstOffset
  private[this] var sequenceNrByPersistenceId: Map[String, Long] = Map.empty
  private[this] var lastSnapshotSequenceNr: Long = 0L
  private[this] var _noOfEventsSinceLastSnapshot: Long = 0L
  private[this] var currentState: State = State.WaitingForSnapshot
  private[this] var loadSnapshotTimer: Option[Cancellable] = None
  private[this] var savingSnapshot: Boolean = false

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
  def recoveringStream(): Source[AnyRef, _]

  /**
    * It is the source od EventEnvelope used to receive live events, it MUST be a infinite stream (eg: It should never
    * complete)
    *
    * It is declared as AnyRef to be able to return [[EventEnvelope]] or [[EventEnvelope2]].
    */
  def liveStream(): Source[AnyRef, _]

  /**
    * It is an hook called before the actor switch to live mode. It is synchronous (it can change the actor status).
    * It can be useful to fetch additional data from other actor/services before starting receiving messages.
    */
  def preLive(): Unit = {}

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
  final def lastOffset: Offset = _lastOffset

  /**
    * Return the next sequence nr to fetch from the journal for the given persistence ID.
    */
  final def nextSequenceNr(persistenceId: String): Long = sequenceNrByPersistenceId.getOrElse(persistenceId, 0L)

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
    super.aroundPreStart()
    loadSnapshot(snapshotterId, SnapshotSelectionCriteria.Latest, Long.MaxValue)
    // If the `loadSnapshotTimeout` is finite, it makes sure the Actor will not get stuck in 'waitingForSnapshot' state.
    loadSnapshotTimer = loadSnapshotTimeout match {
      case timeout: FiniteDuration ⇒
        Some(
          context.system.scheduler.scheduleOnce(
            timeout,
            self,
            LoadSnapshotFailed(new TimeoutException(s"Load snapshot timeout after $timeout"))
          )
        )
      case _ ⇒
        None
    }
    currentState = State.WaitingForSnapshot
  }

  override protected[akka] def aroundPostStop(): Unit = {
    loadSnapshotTimer.foreach(_.cancel())
    materializer.shutdown()
    super.aroundPostStop()
  }

  override protected[akka] def aroundReceive(behaviour: Receive, msg: Any): Unit =
    if (isWaitingForSnapshot) {
      waitingForSnapshot(behaviour, msg)
    } else if (isRecovering) {
      recovering(behaviour, msg)
    } else {
      assert(isLive)
      live(behaviour, msg)
    }

  private def live(behaviour: Receive, msg: Any) =
    msg match {
      case StartLive =>
        sender() ! EventReplayed

      case EventEnvelope2(offset, persistenceId, sequenceNr, event) ⇒
        _lastOffset = offset
        sequenceNrByPersistenceId = sequenceNrByPersistenceId + (persistenceId -> (sequenceNr + 1))
        _noOfEventsSinceLastSnapshot = _noOfEventsSinceLastSnapshot + 1
        super.aroundReceive(behaviour, event)
        sender() ! EventReplayed

      case EventEnvelope(offset, persistenceId, sequenceNr, event) ⇒
        _lastOffset = Sequence(offset)
        sequenceNrByPersistenceId = sequenceNrByPersistenceId + (persistenceId -> (sequenceNr + 1))
        _noOfEventsSinceLastSnapshot = _noOfEventsSinceLastSnapshot + 1
        super.aroundReceive(behaviour, event)
        sender() ! EventReplayed

      case LiveStreamFailed(ex) ⇒
        log.error(ex, s"Live stream failed, it is a fatal error")
        // We have to crash the actor
        throw ex

      case msg @ SaveSnapshotSuccess(metadata) ⇒
        snapshotSaved(metadata)
        super.aroundReceive(behaviour, msg)

      case msg @ SaveSnapshotFailure(metadata, error) ⇒
        snapshotSavingFailed(metadata, error)
        super.aroundReceive(behaviour, msg)

      case _ ⇒
        _noOfEventsSinceLastSnapshot = _noOfEventsSinceLastSnapshot + 1
        super.aroundReceive(behaviour, msg)
    }

  private def recovering(behaviour: Receive, msg: Any) =
    msg match {
      case StartRecovery =>
        sender() ! EventReplayed

      case EventEnvelope(offset, persistenceId, sequenceNr, event) ⇒
        _lastOffset = Sequence(offset)
        sequenceNrByPersistenceId = sequenceNrByPersistenceId + (persistenceId -> (sequenceNr + 1))
        _noOfEventsSinceLastSnapshot = _noOfEventsSinceLastSnapshot + 1
        super.aroundReceive(behaviour, event)
        sender() ! EventReplayed

      case EventEnvelope2(offset, persistenceId, sequenceNr, event) ⇒
        _lastOffset = offset
        sequenceNrByPersistenceId = sequenceNrByPersistenceId + (persistenceId -> (sequenceNr + 1))
        _noOfEventsSinceLastSnapshot = _noOfEventsSinceLastSnapshot + 1
        super.aroundReceive(behaviour, event)
        sender() ! EventReplayed

      case QueryView.RecoveryCompleted ⇒
        startLive()

      case RecoveryFailed(ex) ⇒
        // TODO if it is a Timeout decide if switch to live or crash
        // We have to crash the actor
        throw ex

      case msg @ SaveSnapshotSuccess(metadata) ⇒
        snapshotSaved(metadata)
        super.aroundReceive(behaviour, msg)

      case msg @ SaveSnapshotFailure(metadata, error) ⇒
        snapshotSavingFailed(metadata, error)
        super.aroundReceive(behaviour, msg)

      case _: Any ⇒
        recoveringStash.stash()
    }

  private def waitingForSnapshot(behaviour: Receive, msg: Any) =
    msg match {
      case LoadSnapshotResult(Some(SelectedSnapshot(metadata, status: QueryViewSnapshot[_])), _) ⇒
        val offer = SnapshotOffer(metadata, status.data)
        if (behaviour.isDefinedAt(offer)) {
          super.aroundReceive(behaviour, offer)
          _lastOffset = status.maxOffset
          sequenceNrByPersistenceId = status.sequenceNrs
          lastSnapshotSequenceNr = metadata.sequenceNr
        }
        startRecovery()

      case LoadSnapshotResult(None, _) ⇒
        startRecovery()

      case LoadSnapshotFailed(ex) ⇒
        // It is recoverable so we don't need to crash the actor
        log.error(ex, s"Error loading the snapshot")
        startRecovery()

      case _ ⇒
        recoveringStash.stash()
    }

  private def startRecovery(): Unit = {

    loadSnapshotTimer.foreach(_.cancel())
    currentState = State.Recovering

    // TODO Pass the sequenceNrByPersistenceId and offset to recoveringStream
    val stream = recoveryTimeout match {
      case t: FiniteDuration ⇒ recoveringStream().completionTimeout(t)
      case _ ⇒ recoveringStream()
    }

    val recoverySink =
      Sink.actorRefWithAck(self, StartRecovery, EventReplayed, QueryView.RecoveryCompleted, e => RecoveryFailed(e))

    stream
      .filterAlreadyReceived(sequenceNrByPersistenceId)
      .concat(Source.single(QueryView.RecoveryCompleted))
      .to(recoverySink)
      .run()
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

    liveStream() // TODO Pass the sequenceNrByPersistenceId and offset to liveStream
      .filterAlreadyReceived(sequenceNrByPersistenceId)
      .to(liveSink)
      .run()
  }

  override def saveSnapshot(snapshot: Any): Unit = if (!savingSnapshot) {
    // Decorate the snapshot
    savingSnapshot = true
    super.saveSnapshot(QueryViewSnapshot(snapshot, _lastOffset, sequenceNrByPersistenceId))
  }

  private def snapshotSaved(metadata: SnapshotMetadata): Unit = {
    lastSnapshotSequenceNr = metadata.sequenceNr
    _noOfEventsSinceLastSnapshot = 0L
  }

  private def snapshotSavingFailed(metadata: SnapshotMetadata, error: Throwable): Unit = {
    savingSnapshot = false
    log.error(error, s"Error saving snapshot")
  }

}
