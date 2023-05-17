// Copyright 2018-2021 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

type OpCode uint8

// Operations
const (
	OpPut                   = OpCode(iota) + 1
	OpPutEx                 // 2
	OpPutIf                 // 3
	OpPutIfEx               // 4
	OpGet                   // 5
	OpDelete                // 6
	OpDestroy               // 7
	OpLock                  // 8
	OpLockWithTimeout       // 9
	OpUnlock                // 10
	OpUpdateRouting         // 11
	OpPutReplica            // 12
	OpPutIfReplica          // 13
	OpPutExReplica          // 14
	OpPutIfExReplica        // 15
	OpDeletePrev            // 16
	OpGetPrev               // 17
	OpGetReplica            // 18
	OpDeleteReplica         // 19
	OpDestroyDMapInternal   // 20
	OpMoveFragment          // 21
	OpLengthOfPart          // 22
	OpPipeline              // 23
	OpPing                  // 24
	OpStats                 // 25
	OpExpire                // 26
	OpExpireReplica         // 27
	OpQuery                 // 28
	OpLocalQuery            // 29
	OpPublishDTopicMessage  // 30
	OpDestroyDTopicInternal // 31
	OpDTopicPublish         // 32
	OpDTopicAddListener     // 33
	OpDTopicRemoveListener  // 34
	OpDTopicDestroy         // 35
	OpCreateStream          // 36
	OpStreamCreated         // 37
	OpStreamMessage         // 38
	OpStreamPing            // 39
	OpStreamPong            // 40
	OpFunction              // 41
)

type StatusCode uint8

// Status Codes
const (
	StatusOK                  = StatusCode(iota) + 1
	StatusErrInternalFailure  // 2
	StatusErrKeyNotFound      // 3
	StatusErrNoSuchLock       // 4
	StatusErrLockNotAcquired  // 5
	StatusErrWriteQuorum      // 6
	StatusErrReadQuorum       // 7
	StatusErrOperationTimeout // 8
	StatusErrKeyFound         // 9
	StatusErrClusterQuorum    // 10
	StatusErrUnknownOperation // 11
	StatusErrEndOfQuery       // 12
	StatusErrServerGone       // 13
	StatusErrInvalidArgument  // 14
	StatusErrKeyTooLarge      // 15
	StatusErrNotImplemented   // 16
)
