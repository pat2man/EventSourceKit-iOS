//
//  MessageManager.swift
//  EventSourceKit
//
//  Created by Patrick Tescher on 8/17/15.
//  Copyright © 2015 Ticketfly. All rights reserved.
//

import Foundation
import PromiseKit

//MARK: Errors

let noEntityError = NSError(
    domain: "com.ticketfly.eventsourcekit",
    code: 404,
    userInfo: [NSLocalizedDescriptionKey:"No message entity"]
)

let noMatchingMessageParsersError = NSError(
    domain: "com.ticketfly.eventsourcekit",
    code: 404,
    userInfo: [NSLocalizedDescriptionKey:"No matching message parsers found"]
)

public protocol EventSnapshotter {
    func appliesToEvent(event: Event) -> Bool
    func takeSnapshot(event: Event) -> Promise<Snapshot>
    func persist(snapshot: Snapshot) -> Promise<Snapshot>
}

public protocol MessageParser {
    func matchesMessage(message: Message) -> Bool
    func parseMessage(message: Message) -> Promise<Event>
}

public protocol EventStore {
    func insertEvent(event: Event) -> Promise<Event>
    func finalizeTransaction() -> Promise<[Event]>
}

public protocol Message {
    var messageID: String { get }
    var topic: String { get }
    var body: NSData { get }
}

public protocol Event {
    var eventID: String { get }
    var aggregationKey: String { get }
    var dictionary: [String: AnyObject] { get }
}

extension NSDictionary: Event {
    public var eventID: String {
        get {
            if let eventID = self["eventID"] as? String {
                return eventID
            } else {
                return ""
            }
        }
    }
    
    public var aggregationKey: String {
        get {
            if let aggregationKey = self["aggregationKey"] as? String {
                return aggregationKey
            } else {
                return ""
            }
        }
    }
    
    public var dictionary: [String: AnyObject] {
        get {
            return self as! [String: AnyObject]
        }
    }
}

extension NSManagedObject: Event {
    public var eventID: String {
        get {
            if let eventID = self.primitiveValueForKey("eventID") as? String {
                return eventID
            } else {
                let primitiveEventID = self.primitiveValueForKey("eventID")
                print("messageID is not a string, it is \(primitiveEventID)")
                return ""
            }
        }
    }
    
    public var aggregationKey: String {
        get {
            if let aggregationKey = self.primitiveValueForKey("aggregationKey") as? String {
                return aggregationKey
            } else {
                return ""
            }
        }
    }
    
    public var dictionary: [String: AnyObject] {
        get {
            return [
                "messageID": self.valueForKey("messageID")!,
                "aggregationKey": self.valueForKey("aggregationKey")!
            ]
        }
    }

}

public protocol Snapshot {
    
}

public class MessageManager {
    
    let messageStore: EventStore
    
    init(messageStore: EventStore) {
        self.messageStore = messageStore
    }
    
    var messageParsers = [MessageParser]()
    var messageSnapshotters = [EventSnapshotter]()
    
    public func handleMessage(message: Message) -> Promise<[Snapshot]> {
        return firstly {
            self.parseMessage(message)
        }.then { parsedMessage in
            self.applyMessageActions(parsedMessage)
        }
    }
    
    func parseMessage(message: Message) -> Promise<Event> {
        let matchingParsers = messageParsers.filter{ $0.matchesMessage(message) }
        if let messageParser = matchingParsers.first {
            return messageParser.parseMessage(message)
        } else {
            return Promise { (fulfill, reject) in
                reject(noMatchingMessageParsersError)
            }
        }
    }
    
    func applyMessageActions(message: Event) -> Promise<[Snapshot]> {
        return firstly {
            messageStore.insertEvent(message)
        }.then { insertedMessage in
            self.runSnapshots(message)
        }.then { snapshots in
            self.persistSnapshotMessages(snapshots)
        }
    }

    func runSnapshots(message: Event) -> Promise<[Snapshot]> {
        return when( messageSnapshotters.filter{
            $0.appliesToEvent(message)
        }.map { snapshotter in
            return firstly {
                snapshotter.takeSnapshot(message)
            }.then { snapshot in
                return snapshotter.persist(snapshot)
            }
        })
    }
    
    func persistSnapshotMessages(snapshots: [Snapshot]) -> Promise<[Snapshot]> {
        return firstly {
            messageStore.finalizeTransaction()
        }.then { messageStore in
            return Promise { (fulfill, reject) in
                fulfill(snapshots)
            }
        }
    }
    
}