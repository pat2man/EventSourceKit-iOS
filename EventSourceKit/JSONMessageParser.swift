//
//  RestKitMessageParser.swift
//  EventSourceKit
//
//  Created by Patrick Tescher on 8/19/15.
//  Copyright © 2015 Ticketfly. All rights reserved.
//

import Foundation
import PromiseKit

public struct JSONMessageParser: MessageParser {
    
    let topicRegex: NSRegularExpression

    public func matchesMessage(message: Message) -> Bool {
        return topicRegex.matchesInString(message.topic, options: NSMatchingOptions.Anchored, range: NSMakeRange(0, message.topic.characters.count)).count > 0
    }
    
    public func parseMessage(message: Message) -> Promise<Event>  {
        return Promise { (fulfill, reject) in
            do {
              try fulfill(serializeBody(message.body))
            } catch {
                reject(error)
            }
         }
    }
    
    
    func serializeBody(body: NSData) throws -> Event {
        let parsedObject = try NSJSONSerialization.JSONObjectWithData(body, options: .AllowFragments)
        if let dictionary = parsedObject as? NSDictionary {
            let mutableDictionary = dictionary.mutableCopy()
            mutableDictionary.setValue(dictionary["messageID"], forKey: "eventID") 
            
            if let event = mutableDictionary.copy() as? Event {
                return event
            }
        }
        throw NSError(domain: "com.ticketfly.eventsourcekit", code: 500, userInfo: [NSLocalizedDescriptionKey: "Resulting object is not a Message"])
    }
}