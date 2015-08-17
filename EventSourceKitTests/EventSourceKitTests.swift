//
//  EventSourceKitTests.swift
//  EventSourceKitTests
//
//  Created by Patrick Tescher on 8/17/15.
//  Copyright © 2015 Ticketfly. All rights reserved.
//

import XCTest
import PromiseKit
@testable import EventSourceKit

struct TestCount: Snapshot {
    var count: Int
}

class TestCounter: EventSnapshotter {
    
    let context: NSManagedObjectContext
    let messageContext: NSManagedObjectContext
    
    init(context: NSManagedObjectContext, messageContext: NSManagedObjectContext) {
        self.context = context
        self.messageContext = messageContext
    }
    
    func appliesToEvent(event: Event) -> Bool {
        if let _ = event as? NSDictionary {
            return true
        }
        return false
    }
    
    func takeSnapshot(event: Event) -> Promise<Snapshot> {
        return Promise { (fulfill, reject) in
            let fetchRequest = NSFetchRequest(entityName: "Event")
            fetchRequest.predicate = NSPredicate(format: "aggregationKey == %@", event.aggregationKey)
            messageContext.performBlock {
                do {
                    if let messageObjects = try self.messageContext.executeFetchRequest(fetchRequest) as? [NSManagedObject] {
                        let count = messageObjects.count
                        fulfill(TestCount(count: count))
                    } else {
                        fulfill(TestCount(count: 0))
                    }
                } catch {
                    reject(error)
                }
            }
        }
    }
    
    func persist(snapshot: Snapshot) -> Promise<Snapshot> {
        return Promise {fulfill, reject in
            do {
                try context.save()
                fulfill(snapshot)
            } catch {
                reject(error)
            }
        }
    }
}

struct TestMessage: Message {
    let messageID: String
    let topic = "/registers/1"
    var body: NSData {
        get {
            do {
                let testData = ["test": "true", "aggregationKey": "test", "messageID": messageID]
                return try NSJSONSerialization.dataWithJSONObject(NSDictionary(dictionary: testData), options: NSJSONWritingOptions.PrettyPrinted)
            } catch {
                return NSData()
            }
        }
    }
}

struct CoreDataEventStore: EventStore {
    let context: NSManagedObjectContext
    
    func findExistingEvent(eventID: String) throws -> NSManagedObject? {
        let fetchRequest = NSFetchRequest(entityName: "Event")
        fetchRequest.predicate = NSPredicate(format: "eventID == %@", eventID)
        let results = try context.executeFetchRequest(fetchRequest)
        return results.first as? NSManagedObject
    }
    
    func insertEvent(event: Event) -> Promise<Event> {
        return Promise {fulfill, reject in
            context.performBlock {
                do {
                    let existingObject = try self.findExistingEvent(event.eventID)
                    if let existingObject = existingObject {
                        fulfill(existingObject)
                    } else {
                        print("No existing message matches \(event.eventID)")
                        let newObject = NSEntityDescription.insertNewObjectForEntityForName("Event", inManagedObjectContext: self.context)
                        let dictionaryValues = ["eventID": event.eventID, "aggregationKey": event.aggregationKey, "body": event.dictionary] as [String: AnyObject]
                        newObject.setValuesForKeysWithDictionary(dictionaryValues)
                        try newObject.validateForInsert()
                        fulfill(newObject)
                    }
                } catch {
                    reject(error)
                }
            }
        }
    }
    
    func finalizeTransaction() -> Promise<[Event]> {
        return Promise {fulfill, reject in
            do {
                let events = Array(context.insertedObjects) as [Event]
                try context.save()
                fulfill(events)
            } catch {
                reject(error)
            }
        }
    }
}

class EventSourceKitTests: XCTestCase {
    
    var manager: MessageManager?
    
    override func setUp() {
        super.setUp()
        
        let context = NSManagedObjectContext(concurrencyType: NSManagedObjectContextConcurrencyType.PrivateQueueConcurrencyType)
        
        if let model = NSManagedObjectModel.mergedModelFromBundles(NSBundle.allBundles()) {
            let persistentStoreCoordinator = NSPersistentStoreCoordinator(managedObjectModel: model)
            do {
                try persistentStoreCoordinator.addPersistentStoreWithType(NSInMemoryStoreType, configuration: nil, URL: nil, options: nil)
            } catch {
                XCTFail("Error adding persistent store: \(error)")
            }
            context.persistentStoreCoordinator = persistentStoreCoordinator
            
        } else {
            XCTFail("No model")
        }
        
        let newManager = MessageManager(messageStore: CoreDataEventStore(context: context))
        
        do {
            let regex = try NSRegularExpression(pattern:  "/registers/.*", options: NSRegularExpressionOptions.AllowCommentsAndWhitespace)
            newManager.messageParsers.append(JSONMessageParser(topicRegex:  regex))
            newManager.messageSnapshotters.append(TestCounter(context: context, messageContext:context))
        } catch {
            XCTFail("Error: \(error)")
        }
        
        manager = newManager
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testSingleMessage() {
        let message = TestMessage(messageID: NSUUID().UUIDString)
        print("Generated message with id \(message.messageID)")

        self.measureBlock {

            let expectation = self.expectationWithDescription("Message shoudld be parsed")
            
            if let manager = self.manager {
                firstly {
                    manager.handleMessage(message)
                }.then { (aggregations) -> Void in
                    expectation.fulfill()
                    XCTAssert(aggregations.count == 1, "Should have one snapshot, not \(aggregations.count)")
                    if let testCount = aggregations.first as? TestCount {
                        XCTAssert(testCount.count == 1, "We should only have one test event, not \(testCount.count)")
                    } else {
                        XCTFail("Snapshot was not a test count")
                    }
                }.recover { error in
                    XCTFail("\(error)")
                }
            }
            
            self.waitForExpectationsWithTimeout(10.0) { (error) in
                print("Timed out waiting for message to parse")
            }
        }
    }
    
    func testMultipleMessages() {
        let messageOne = TestMessage(messageID: NSUUID().UUIDString)
        let messageTwo = TestMessage(messageID: NSUUID().UUIDString)

        self.measureBlock {

            let expectation = self.expectationWithDescription("Message shoudld be parsed")
            
            if let manager = self.manager {
                firstly {
                    manager.handleMessage(messageOne)
                }.then { (aggregations) in
                    manager.handleMessage(messageTwo)
                }.then { (snapshots) -> Void in
                    expectation.fulfill()
                    XCTAssert(snapshots.count == 1, "Should have one snapshot, not \(snapshots.count)")
                    if let testCount = snapshots.first as? TestCount {
                        XCTAssert(testCount.count == 2, "We should have two test events, not \(testCount.count)")
                    } else {
                        XCTFail("Snapshot was not a test count")
                    }
                }.recover { error in
                    XCTFail("\(error)")
                }
            }
            
            self.waitForExpectationsWithTimeout(10.0) { (error) in
                print("Timed out waiting for message to parse")
            }
        }
    }

    
}
