//
//  CoreDataEventStore.swift
//  EventSourceKit
//
//  Created by Patrick Tescher on 1/8/16.
//  Copyright © 2016 Ticketfly. All rights reserved.
//

import CoreData
import PromiseKit

public class CoreDataEventStore: EventStore {
    let context: NSManagedObjectContext
    
    init(context: NSManagedObjectContext) {
        self.context = context
    }
    
    func findExistingEvent(eventID: String) -> NSManagedObject? {
        let fetchRequest = NSFetchRequest(entityName: "Event")
        fetchRequest.predicate = NSPredicate(format: "eventID == %@", eventID)
        var results: [AnyObject]?
        let context = self.context
        context.performBlock {
            do {
                results = try context.executeFetchRequest(fetchRequest)
            } catch {
                results = [AnyObject]()
            }
        }
        return results?.first as? NSManagedObject
    }
    
    public func insertEvent(event: Event) -> Promise<Event> {
        return Promise {fulfill, reject in
            context.performBlock {
                do {
                    let existingObject = self.findExistingEvent(event.eventID)
                    if let existingObject = existingObject {
                        fulfill(existingObject)
                    } else {
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
    
    public func finalizeTransaction() -> Promise<[Event]> {
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
