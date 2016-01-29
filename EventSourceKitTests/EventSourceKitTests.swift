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
            let context = self.messageContext
            context.performBlock {
                let fetchRequest = NSFetchRequest(entityName: "Event")
                fetchRequest.predicate = NSPredicate(format: "aggregationKey == %@", event.aggregationKey)
                do {
                    let fetchedObjects = try context.executeFetchRequest(fetchRequest)
                    if let messageObjects = fetchedObjects as? [NSManagedObject] {
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
            let context = self.context
            context.performBlock {
                do {
                    try context.save()
                    fulfill(snapshot)
                } catch {
                    reject(error)
                }
            }
        }
    }
}

struct TestMessage: Message {
    let messageID: String
    let topic = "/registers/1"
    let aggregationKey: String
    var body: NSData {
        get {
            do {
                let testData = ["test": "true", "aggregationKey": aggregationKey, "messageID": messageID]
                return try NSJSONSerialization.dataWithJSONObject(NSDictionary(dictionary: testData), options: NSJSONWritingOptions.PrettyPrinted)
            } catch {
                return NSData()
            }
        }
    }
}

class EventSourceKitTests: XCTestCase {
    
    var manager: MessageManager?
    var managedObjectContext: NSManagedObjectContext?
    
    override func setUp() {
        super.setUp()
        
        let context = NSManagedObjectContext(concurrencyType: .PrivateQueueConcurrencyType)
        
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
        managedObjectContext = context
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testSingleMessage() {

        self.measureBlock {

            let expectation = self.expectationWithDescription("Message shoudld be parsed")
            
            let message = TestMessage(messageID: NSUUID().UUIDString, aggregationKey: NSUUID().UUIDString)
            
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
                XCTAssertNil(error, "Timed out waiting for message to parse: \(error)")
            }
        }
    }
    
    func testMultipleMessages() {
        
        self.measureBlock {

            let expectation = self.expectationWithDescription("Message shoudld be parsed")
            
            if let manager = self.manager {
    
                let messageCount = 150
                
                let aggregationKey = NSUUID().UUIDString
                
                let messagePromises = (1...messageCount).map {_ -> Promise<[Snapshot]> in
                    let message = TestMessage(messageID: NSUUID().UUIDString, aggregationKey: aggregationKey)
                    return manager.handleMessage(message)
                }
                
                when(messagePromises).then { (promiseResults) -> Void in
                    if let snapshots = promiseResults.last {
                        expectation.fulfill()
                        XCTAssert(snapshots.count == 1, "Should have one snapshot, not \(snapshots.count)")
                        if let testCount = snapshots.last as? TestCount {
                            XCTAssertEqual(testCount.count, messageCount, "We should have \(messageCount) test events, not \(testCount.count)")
                        } else {
                            XCTFail("Snapshot was not a test count")
                        }
                    } else {
                        XCTFail("No snapshots")
                    }
                }.recover { error in
                    XCTFail("\(error)")
                }
            }
            
            self.waitForExpectationsWithTimeout(60.0) { (error) in
                XCTAssertNil(error, "Timed out waiting for message to parse: \(error)")
            }
        }
    }

    
}
