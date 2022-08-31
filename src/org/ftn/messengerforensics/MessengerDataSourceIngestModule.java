/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.ftn.messengerforensics;



import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sleuthkit.autopsy.casemodule.Case;
import org.sleuthkit.autopsy.casemodule.NoCurrentCaseException;

import org.sleuthkit.autopsy.ingest.DataSourceIngestModuleProgress;
import org.sleuthkit.autopsy.ingest.IngestModule;

import org.sleuthkit.datamodel.TskCoreException;

import org.sleuthkit.autopsy.ingest.DataSourceIngestModule;
import org.sleuthkit.autopsy.ingest.IngestJobContext;

import org.sleuthkit.autopsy.coreutils.Logger;
import org.sleuthkit.autopsy.coreutils.AppSQLiteDB;

import org.sleuthkit.datamodel.*;
import org.sleuthkit.datamodel.blackboardutils.*;
import org.sleuthkit.datamodel.blackboardutils.CommunicationArtifactsHelper.CallMediaType;
import org.sleuthkit.datamodel.blackboardutils.CommunicationArtifactsHelper.CommunicationDirection;
import org.sleuthkit.datamodel.blackboardutils.attributes.MessageAttachments;
import org.sleuthkit.datamodel.blackboardutils.attributes.MessageAttachments.FileAttachment;
import org.sleuthkit.datamodel.blackboardutils.attributes.MessageAttachments.URLAttachment;
/**
 *
 * @author Natasa
 */
public class MessengerDataSourceIngestModule implements DataSourceIngestModule{
     private static final String moduleName = MessengerIngestModuleFactory.getModuleName();
    private IngestJobContext context;
    private Case currentCase;
    private Blackboard blackboard;
    
    private Logger logger = Logger.getLogger(MessengerIngestModuleFactory.getModuleName());;
    private static String FB_MESSENGER_PACKAGE_NAME = "com.facebook.orca";
    private static String MESSAGE_TYPE = "Facebook Messenger";
    private static String VERSION = "";
    private String ownerUserId = null;
    private CommunicationArtifactsHelper communicationArtifactsHelperAccounts = null;
    private CommunicationArtifactsHelper communicationArtifactsHelperThreads = null;

    public MessengerDataSourceIngestModule(){
        
    }
    @Override
    public void startUp(IngestJobContext context) throws IngestModuleException {
        this.context = context;
         
         try {
            currentCase = Case.getCurrentCaseThrows();
            blackboard = currentCase.getServices().getArtifactsBlackboard();
            
        } catch (NoCurrentCaseException ex) {
            logger.log(Level.SEVERE, "Exception while getting open case.", ex);
            throw new IngestModuleException("Exception while getting open case.", ex);
        }
    }

    @Override
    public ProcessResult process(Content content, DataSourceIngestModuleProgress dsimp) {
        logger.log(Level.INFO,"Started proccessing...");
        dsimp.switchToIndeterminate();
        Collection<AppSQLiteDB> threadsDbs;
        Collection<AppSQLiteDB> searchCacheDbs;
        Collection<AppSQLiteDB> msysDbs;
        Collection<AppSQLiteDB> prefsDbs;
         
        try{

            prefsDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "prefs_db", true, FB_MESSENGER_PACKAGE_NAME);
            threadsDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "threads_db2", true,FB_MESSENGER_PACKAGE_NAME);
            searchCacheDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "search_cache_db", true,FB_MESSENGER_PACKAGE_NAME);
            msysDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "msys_database", false,FB_MESSENGER_PACKAGE_NAME);
            logger.log(Level.INFO," prefsDbs count: " + prefsDbs.size());
            logger.log(Level.INFO," threadsDbs count: " + threadsDbs.size());
            logger.log(Level.INFO," searchCacheDbs count: " + searchCacheDbs.size());
            logger.log(Level.INFO," msysDbs count " + msysDbs.size());
            
            getAppVersion(prefsDbs, content);
            logger.log(Level.INFO,"App Version: " + VERSION);
            
            for (AppSQLiteDB msysDb : msysDbs) {
                logger.log(Level.INFO," DbFilename: " + msysDb.getDBFile().getName());
                
                try{
                    extractOwnerFacebookId(msysDb);
//                    if(communicationArtifactsHelperAccounts == null){
//                        if(ownerUserId == null){  
//                            communicationArtifactsHelperAccounts  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(), moduleName, content, Account.Type.FACEBOOK);
//                        }else{
//                            logger.log(Level.INFO,"ownerUserId: " + ownerUserId);
//                            communicationArtifactsHelperAccounts  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(), moduleName, content, Account.Type.FACEBOOK,  Account.Type.FACEBOOK, ownerUserId);
//                        }
//                    }
//   
                    //analyzeContacts(msysDb, content);
                    //analyzeStories(msysDb, content);
                    
                    
                }catch(Exception exception){
                    logger.log(Level.WARNING, exception.getMessage(), exception);
                }finally{
                    msysDb.close();
                }
            }
            
            for (AppSQLiteDB threadsDb : threadsDbs) {
                logger.log(Level.INFO," DbFilename: " + threadsDb.getDBFile().getName());
                try{
                    if(communicationArtifactsHelperThreads == null){
                        if(ownerUserId == null){
                            communicationArtifactsHelperThreads  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(), moduleName, content, Account.Type.FACEBOOK);
                        }else{
                            communicationArtifactsHelperThreads  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(), moduleName, content, Account.Type.FACEBOOK,  Account.Type.FACEBOOK, ownerUserId);
                        }
                    }
                    //analyzeMessages(threadsDb, content);
                    analyzeCallLogs(threadsDb);
                }catch(Exception exception){
                    logger.log(Level.WARNING, exception.getMessage(), exception);
                }finally{
                    threadsDb.close();
                }
            }
            
            for (AppSQLiteDB searchCacheDb : searchCacheDbs) {
                logger.log(Level.INFO," DbFilename: " + searchCacheDb.getDBFile().getName());
                try{
                   analyzeSearchItems(searchCacheDb, content);
                   analyzeRecentSearchItems(searchCacheDb, content);
                }catch(Exception exception){
                    logger.log(Level.WARNING, exception.getMessage(), exception);
                }finally{
                    searchCacheDb.close();
                }
            }
            
            logger.log(Level.INFO,"Finished proccessing...");
            return IngestModule.ProcessResult.OK;
            
        }catch(Exception exception){
             logger.log(Level.SEVERE,exception.getMessage(), exception);
             return IngestModule.ProcessResult.ERROR;
        }
    }

    private void getAppVersion(Collection<AppSQLiteDB> prefsDbs, Content content) throws TskCoreException{
        logger.log(Level.INFO,"Method start: getAppVersion");
        String query = "SELECT key, type, value FROM preferences WHERE key LIKE '%app_version_name_current%'";
        
        for (AppSQLiteDB appSQLiteDB : prefsDbs){      
            try{
                ResultSet resultSet = appSQLiteDB.runQuery(query);
                if(resultSet != null){
                     while (resultSet.next()){
                        VERSION = resultSet.getString("value");
                    }
                }
                
            }catch(SQLException exception){
                logger.log(Level.WARNING, exception.getMessage() ,exception);
            }finally{
                appSQLiteDB.close();
            }
        }
        logger.log(Level.INFO,"Method end: getAppVersion");
    }
    
    private void analyzeContacts(AppSQLiteDB msysDb, Content content) {
        logger.log(Level.INFO,"Method start: analyzeContacts");
        String query = "select id, name, profile_picture_url, phone_number, email_address, work_company_name, birthday_timestamp, username, blocked_by_viewer_status, blocked_since_timestamp_ms from contacts";
            
            try{
                ResultSet resultSet = msysDb.runQuery(query);
            
                if(resultSet != null){
                    Collection<BlackboardArtifact> artifacts = new ArrayList<>();
                     while (resultSet.next()){
                        String email = resultSet.getString("email_address")!= null ? resultSet.getString("email_address") : "";
                        String phoneNumber = resultSet.getString("phone_number") != null ? resultSet.getString("phone_number") : "";
                        String blockedBy = ""; 
                        if(resultSet.getInt("blocked_by_viewer_status") == 2){
                            blockedBy = "OWNER_USER";
                        }else if(resultSet.getInt("blocked_by_viewer_status") == 1){
                             blockedBy = "UNKNOWN";
                        }
                        
                        long blockedSince = resultSet.getLong("blocked_since_timestamp_ms") / 1000;
                        
                        ArrayList<BlackboardAttribute> attributes = new ArrayList<>();
       
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, moduleName, resultSet.getString("id")));
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME, moduleName, resultSet.getString("name")));
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME, moduleName, resultSet.getString("username")));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, resultSet.getString("profile_picture_url")));
                        
                        
                        BlackboardAttribute.Type workCompanyAttributeType = currentCase.getSleuthkitCase().getAttributeType("WORK_COMPANY_NAME");
                        if (workCompanyAttributeType == null) {
                                workCompanyAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("WORK_COMPANY_NAME", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Work Company Name");
                        }
                        
                        BlackboardAttribute.Type blockedAttributeType = currentCase.getSleuthkitCase().getAttributeType("BLOCKED_BY_VIEWER_STATUS");
                        if (blockedAttributeType == null) {
                                blockedAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("BLOCKED_BY_VIEWER_STATUS", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Blocked by");
                        }
                        
                        attributes.add( new BlackboardAttribute(workCompanyAttributeType, moduleName, resultSet.getString("work_company_name") != null ? resultSet.getString("work_company_name"): "" ));
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, blockedSince));
                        attributes.add( new BlackboardAttribute(blockedAttributeType, moduleName, blockedBy));
                        
//                         communicationArtifactsHelperAccounts.addContact(resultSet.getString("name"),
//                                                    phoneNumber,
//                                                    "",
//                                                    "",
//                                                    email,
//                                                    additionalAttributes);
                        BlackboardArtifact artifact = content.newDataArtifact(BlackboardArtifact.Type.TSK_CONTACT, attributes);
                        artifacts.add(artifact);
                       
                        
                    }
                     
                      blackboard.postArtifacts(artifacts, moduleName);
                }          
            }catch(SQLException exception){
                logger.log(Level.WARNING, exception.getMessage(), exception);
            }catch(TskCoreException exception){
                logger.log(Level.WARNING, exception.getMessage(), exception);
            } catch (TskDataException exception) {
                logger.log(Level.WARNING, exception.getMessage(), exception);
            } catch (Blackboard.BlackboardException exception) {
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }
        logger.log(Level.INFO,"Method end: analyzeContacts");
    }
    
    private void analyzeRecentSearchItems(AppSQLiteDB searchCacheDb, Content content) {
        logger.log(Level.INFO,"Method start: analyzeRecentSearchItems");
        String query = "select fbid, item_type, display_name, first_name, last_name, picture_url, most_recent_pick_time_ms, total_pick_count from recent_search_items";

            try{
                ResultSet searchItemsSet = searchCacheDb.runQuery(query);
            
                if(searchItemsSet != null){
                    ArrayList<BlackboardArtifact> artifacts = new ArrayList<>();
                    ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                     while (searchItemsSet.next()){
                        String fbid = searchItemsSet.getString("fbid");
                        String itemType = searchItemsSet.getString("item_type");
                        String displayName = searchItemsSet.getString("display_name");
                        String firstName = searchItemsSet.getString("first_name");
                        String lastname = searchItemsSet.getString("last_name");
                        String pictureUrl = searchItemsSet.getString("picture_url");

                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, moduleName, fbid));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, displayName));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, pictureUrl));

                        BlackboardAttribute.Type itemTypeAttributeType = currentCase.getSleuthkitCase().getAttributeType("ITEM_TYPE");
                        if (itemTypeAttributeType == null) {
                                itemTypeAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("ITEM_TYPE", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Item type");
                        }
                        
                        attributes.add( new BlackboardAttribute(itemTypeAttributeType, moduleName, itemType));
                         //TODO: Add attributes for most_recent_pick_time_ms, total_pick_count
                        BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                             "RECENT_SEARCH_ITEMS", "Recent Search Items", BlackboardArtifact.Category.DATA_ARTIFACT);

                        BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                        artifacts.add(artifact);
                        
                    }
                     
                    
                    blackboard.postArtifacts(artifacts, moduleName);
                     
                }
                
            }catch(SQLException exception){
                logger.log(Level.SEVERE, exception.getMessage() ,exception);
            }catch(TskDataException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }catch(TskCoreException exception){
                logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
            }catch(Blackboard.BlackboardException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }
        
        logger.log(Level.INFO,"Method end: analyzeRecentSearchItems");
    }
    
    private CommunicationDirection findCommunicationDirection(String senderId){
        CommunicationDirection direction = CommunicationDirection.UNKNOWN;
        if(senderId != null){
            if (senderId != ownerUserId){
                direction = CommunicationDirection.INCOMING;
            }else{
                direction = CommunicationDirection.OUTGOING;
            }
        }
        
        return direction;
    }
    
    private String extractSenderId(String json){
        logger.log(Level.INFO,"Method start: extractSenderId");
        logger.log(Level.INFO,json);
        String senderId = "";
        if(json != null && json != ""){
            
            Pattern pattern = Pattern.compile(".*FACEBOOK:(\\d+).*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderId = matcher.group(1);
            }
         }
        return senderId;
    }
    
    private String extractSenderEmail(String json){
    logger.log(Level.INFO,"Method start: extractSenderEmail");
    logger.log(Level.INFO,json);
    String senderEmail = "";
    if(json != null && json != ""){

         Pattern pattern = Pattern.compile(".*email\":\"?(.*?)\"?,\".*");
        Matcher matcher = pattern.matcher(json);
        if (matcher.matches()) {
            senderEmail = matcher.group(1);
        }
     }
        return senderEmail;
    }
    
    private String extractSenderName(String json){
        logger.log(Level.INFO,"Method start: extractSenderName");
        logger.log(Level.INFO,json);
        String senderName = "";
        if(json != null && json != ""){
            Pattern pattern = Pattern.compile(".*name\":\"?(.*?)\"?,\".*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderName = matcher.group(1);
            }
         }
        return senderName;
    }
    
    private String extractSenderPhone(String json){
        logger.log(Level.INFO,"Method start: extractSenderName");
        logger.log(Level.INFO,json);
        String senderPhone = "";
        if(json != null && json != ""){
           Pattern pattern = Pattern.compile(".*phone\":\"?(.*?)\"?,\".*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderPhone = matcher.group(1);
            }
         }
        return senderPhone;
    }
    
    private String extractRecepientId(String user_key, String senderId){
        String recepient = "";
        if(user_key != null && user_key != ""){
          Pattern pattern = Pattern.compile("(?i)\\w+:(\\d+)");
          Matcher matcher = pattern.matcher(user_key);
          if (matcher.matches()) {
             recepient = matcher.group(1);
          }
        }
          
        return recepient;
    }
    
    private void analyzeMessages(AppSQLiteDB threadsDb, Content content) throws TskCoreException, Blackboard.BlackboardException{
        logger.log(Level.INFO,"Method start: analyzeMessages");
                try{
                 
                    String query = "SELECT msg_id, text, sender, shares, timestamp_ms, msg_type, messages.thread_key as thread_key, snippet, thread_participants.user_key as user_key, thread_users.name as name, attachments, pending_send_media_attachment FROM messages JOIN thread_participants ON messages.thread_key = thread_participants.thread_key JOIN thread_users ON thread_participants.user_key = thread_users.user_key WHERE msg_type not in (9, 203) ORDER BY msg_id";
                    ResultSet threadsResultSet = threadsDb.runQuery(query);
                     
                    String oldMsgId = null;

                    CommunicationDirection direction = CommunicationDirection.UNKNOWN;
                    String fromId = null;
                    List<String> recipientIdsList = new ArrayList<>();
                    long timeStamp = -1;
                    String msgText = "";
                    String threadId = "";
                    MessageAttachments messageAttachments = null;
                    String attachment = "";
                    String pendingSendMediaAttachment = "";
                    String senderName = "";
                    String shares = "";
                    String snnipet = "";
                    
                    while (threadsResultSet.next()) {
                        String msgId = threadsResultSet.getString("msg_id");

                        if(msgId != oldMsgId){

                            if(oldMsgId != null){
                                BlackboardArtifact messageArtifact = communicationArtifactsHelperThreads.addMessage(MESSAGE_TYPE, direction, fromId, recipientIdsList,timeStamp,
                                                                     CommunicationArtifactsHelper.MessageReadStatus.UNKNOWN, "", msgText, threadId);

                                recipientIdsList = new ArrayList<>();
                                if(messageAttachments != null){
                                     communicationArtifactsHelperThreads.addAttachments(messageArtifact, messageAttachments);
                                     messageAttachments = null;
                                }
                            }

                            oldMsgId = msgId;


                            threadId = threadsResultSet.getString("thread_key");
                            timeStamp = threadsResultSet.getLong("timestamp_ms")/1000;
                            fromId = extractSenderId(threadsResultSet.getString("sender"));
                            senderName = extractSenderName(threadsResultSet.getString("sender"));

                            direction = findCommunicationDirection(fromId);
                            msgText = threadsResultSet.getString("text") != null ?  threadsResultSet.getString("text") : "";
                            snnipet = threadsResultSet.getString("snippet") != null ? threadsResultSet.getString("snippet"): "";
                            shares = threadsResultSet.getString("shares") != null ? threadsResultSet.getString("shares") : "";
                            attachment = threadsResultSet.getString("attachments");
                            pendingSendMediaAttachment = threadsResultSet.getString("pending_send_media_attachment");
   
                            List<URLAttachment> urlAttachments  = new ArrayList<>();
                            List<FileAttachment> fileAttachments  = new ArrayList<>();
                            
                            if(attachment != null || pendingSendMediaAttachment != null){
                                if(attachment != null){
                                    Pattern pattern = Pattern.compile("(?i)\\\"mime_type\\\":\\\"(\\w+/\\w*)\\\",");
                                    Matcher matcher = pattern.matcher(attachment);
                                    if (matcher.matches()) {
                                        String mimeType = matcher.group(1);
                                        if (mimeType == "image/jpeg" || mimeType == "image/gif"){
                                            Pattern srcPattern = Pattern.compile("src\\\\{3}\\\":\\\\{3}\\\"(http|https[-a-zA-Z0-9@:%._\\\\+~#=&?/]+)\\\\{3}\\\"}\\\\\\\"");
                                            matcher = srcPattern.matcher(attachment);
                                           if (matcher.matches()){                                            
                                               for(int i =1; i <= matcher.groupCount(); i++){
                                                   urlAttachments.add(new URLAttachment(matcher.group(i)));
                                               }
                                               
                                           }
                                        }else if(mimeType == "video/mp4"){
                                            Pattern videoUrlPattern = Pattern.compile("(?i)\\\"video_data_url\\\":\\\"(http|https[-a-zA-Z0-9@:%._\\\\+~#=&?/]+)\\\"");
                                            matcher = videoUrlPattern.matcher(attachment);
                                            if (matcher.matches()){                                            
                                               for(int i =1; i <= matcher.groupCount(); i++){
                                                   urlAttachments.add(new URLAttachment(matcher.group(i)));
                                               } 
                                            }
                                            videoUrlPattern = Pattern.compile("(?i)\\\"video_data_thumbnail_url\\\":\\\"(http|https[-a-zA-Z0-9@:%._\\\\+~#=&?/]+)\\\"");
                                            matcher = videoUrlPattern.matcher(attachment);
                                            if (matcher.matches()){                                            
                                               for(int i =1; i <= matcher.groupCount(); i++){
                                                   urlAttachments.add(new URLAttachment(matcher.group(i)));
                                               } 
                                            }
                                            
                                        }else if(mimeType == "audio/mp4" || mimeType == "audio/mpeg"){
                                            Pattern videoUrlPattern = Pattern.compile("(?i)\\\"audio_uri\\\":\\\"(http|https[-a-zA-Z0-9@:%._\\\\+~#=&?/]+)\\\"");
                                            matcher = videoUrlPattern.matcher(attachment);
                                            if (matcher.matches()){                                            
                                               for(int i =1; i <= matcher.groupCount(); i++){
                                                   urlAttachments.add(new URLAttachment(matcher.group(i)));
                                               } 
                                            }
                                        }else{
                                            logger.log(Level.WARNING, "Attachment type not supported: {0}", mimeType);
                                        }
                                        
                                                                               
                                        
                                    }
                                }
                                if(pendingSendMediaAttachment != null){
                                    // TODO: Deal with this
                                }
                                
                                messageAttachments = new MessageAttachments(fileAttachments, urlAttachments);
                                
                            }

                            String recepientId = extractRecepientId(threadsResultSet.getString("user_key"), fromId);
                            if(fromId != recepientId){
                                recipientIdsList.add(recepientId);
                            }
                            
                        }else{ // same msgId as previous, just collect recipient
                            String recepientId = extractRecepientId(threadsResultSet.getString("user_key"), fromId);
                            if(fromId != recepientId){
                                recipientIdsList.add(recepientId);
                            }
                        }
                    
                    }
                    
//                    communicationArtifactsHelperThreads.addMessage(MESSAGE_TYPE, direction, fromId, recipientIdsList,timeStamp,
//                                                                   CommunicationArtifactsHelper.MessageReadStatus.UNKNOWN, "", msgText, threadId);

                      
                }catch(SQLException exception){
                    logger.log(Level.SEVERE, exception.getMessage() ,exception);
                }catch(TskCoreException exception){
                    logger.log(Level.SEVERE, "Failed to add FB Messenger message artifacts." ,exception);
                }catch(Blackboard.BlackboardException exception){
                    logger.log(Level.SEVERE, "Failed to post artifacts." ,exception);
                }
                
                logger.log(Level.INFO,"Method end: analyzeMessages");
                
    }
    
    private void analyzeCallLogs(AppSQLiteDB threadsDb){
        logger.log(Level.INFO,"Method START: analyzeCallLogs");
        /*
        msg_type indicates type of call:
            9: one to one calls
            203: group call
        1-to-1 calls only have a call_ended record.
        group calls have a call_started_record as well as call_ended recorded, with *different* message ids.
        all the data we need can be found in the call_ended record.
        */
            
        String query = "SELECT msg_id, text, sender, timestamp_ms, msg_type, admin_text_thread_rtc_event, generic_admin_message_extensible_data, messages.thread_key as thread_key, thread_participants.user_key as user_key, thread_users.name as name FROM messages JOIN thread_participants ON messages.thread_key = thread_participants.thread_key JOIN thread_users ON thread_participants.user_key = thread_users.user_key WHERE msg_type = 9 OR (msg_type = 203 AND admin_text_thread_rtc_event = 'group_call_ended') ORDER BY msg_id";
        
            
        try{
            ResultSet messagesResultSet = threadsDb.runQuery(query);
            
            if(messagesResultSet != null){
                
                ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                String oldMsgId = null;
                CommunicationDirection direction = CommunicationDirection.UNKNOWN;
                String callerId = null;
                ArrayList<String> calleeIdsList = new ArrayList<>();
                long startTimeStamp = -1;
                long endTimeStamp = -1;
                int duration = 0; // call duration in seconds
                CallMediaType mediaType = CallMediaType.AUDIO;
                
                 while (messagesResultSet.next()){
                    String msgId = messagesResultSet.getString("msg_id");
                    
                    if(msgId != oldMsgId){
                        if(oldMsgId != null){
                            BlackboardArtifact messageArtifact = communicationArtifactsHelperThreads.addCalllog( 
                                                        direction,
                                                        callerId,
                                                        calleeIdsList,
                                                        startTimeStamp,
                                                        endTimeStamp,
                                                        mediaType,
                                                        attributes);

                            calleeIdsList = new ArrayList<>();
                        }

                        oldMsgId = msgId;
                        
                        callerId = extractSenderId(messagesResultSet.getString("sender"));
                        direction = findCommunicationDirection(callerId);
                        endTimeStamp = messagesResultSet.getLong("timestamp_ms") / 1000;
                        
                        String recepientId = extractRecepientId(messagesResultSet.getString("user_key"), callerId);
                        if(callerId != recepientId){
                            calleeIdsList.add(recepientId);
                        }
                        String senderName = extractSenderName(messagesResultSet.getString("sender"));
                        String senderEmail = extractSenderEmail(messagesResultSet.getString("sender"));
                        String senderPhone = extractSenderPhone(messagesResultSet.getString("sender"));
                        
                        logger.log(Level.INFO,"msgId: " + msgId);
                        logger.log(Level.INFO,"callerId: " + callerId);
                        logger.log(Level.INFO,"direction: " + direction);
                        logger.log(Level.INFO,"recepientId: " + recepientId);
                        logger.log(Level.INFO,"senderName: " + senderName);
                        logger.log(Level.INFO,"senderEmail: " + senderEmail);
                        logger.log(Level.INFO,"senderPhone: " + senderPhone);
                        
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_EMAIL_FROM, moduleName, senderEmail));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, senderName));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM, moduleName, senderPhone));
                        
                        String json = messagesResultSet.getString("generic_admin_message_extensible_data");
                        if(json != null && json.trim() != ""){
                            Pattern pattern = Pattern.compile("\"call_duration\":(\\d+),");
                            Matcher matcher = pattern.matcher(json);
                            if (matcher.matches()) {
                                duration = Integer.parseInt(matcher.group(1));
                            }
                            
                            pattern = Pattern.compile("\"video\":(\\w+),");
                            matcher = pattern.matcher(json);
                             if (matcher.matches()) {
                               mediaType = matcher.group(1).toLowerCase() == "true" ? CallMediaType.VIDEO : CallMediaType.AUDIO;
                            }
                        }
                        
                        startTimeStamp = endTimeStamp - duration;
                        
                        
                    }else{//same msgId as last, just collect callee 
                        String recepientId = extractRecepientId(messagesResultSet.getString("user_key"), callerId);
                        if(callerId != recepientId){
                            calleeIdsList.add(recepientId);
                        }
                    }
                     
                 }
                 
                 BlackboardArtifact callLogArtifact = communicationArtifactsHelperThreads.addCalllog( 
                                                        direction,
                                                        callerId,
                                                        calleeIdsList,
                                                        startTimeStamp,
                                                        endTimeStamp,
                                                        mediaType);
                
                
            }
                
            }catch(SQLException exception){
                logger.log(Level.SEVERE, exception.getMessage() ,exception);
            }catch(TskCoreException exception){
                logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
            }catch(Blackboard.BlackboardException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }
        
        logger.log(Level.INFO,"Method end: analyzeCallLogs");
 
    }
    
    private void analyzeSearchItems(AppSQLiteDB searchCacheDb, Content content) throws TskDataException{
        logger.log(Level.INFO,"Method start: analyzeSearchItems");
        String query = "select fbid, item_type, display_name, first_name, last_name, picture_url from search_items";
 
            try{
                ResultSet searchItemsSet = searchCacheDb.runQuery(query);
            
                if(searchItemsSet != null){
                    ArrayList<BlackboardArtifact> artifacts = new ArrayList<>();
                    ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                    while (searchItemsSet.next()){
                        String fbid = searchItemsSet.getString("fbid");
                        String itemType = searchItemsSet.getString("item_type");
                        String displayName = searchItemsSet.getString("display_name");
                        String firstName = searchItemsSet.getString("first_name");
                        String lastname = searchItemsSet.getString("last_name");
                        String pictureUrl = searchItemsSet.getString("picture_url");

                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_ID, moduleName, fbid));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, displayName));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, pictureUrl));

                        BlackboardAttribute.Type itemTypeAttributeType = currentCase.getSleuthkitCase().getAttributeType("ITEM_TYPE");
                        if (itemTypeAttributeType == null) {
                                itemTypeAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("ITEM_TYPE", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Item type");
                        }
                        
                        attributes.add( new BlackboardAttribute(itemTypeAttributeType, moduleName, itemType));
                        BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                             "SEARCH_ITEMS", "Search Items", BlackboardArtifact.Category.DATA_ARTIFACT);

                        BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                        artifacts.add(artifact);
                    }

                    blackboard.postArtifacts(artifacts, moduleName);
                     
                }
                
            }catch(SQLException exception){
                logger.log(Level.SEVERE, exception.getMessage() ,exception);
            }catch(TskDataException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }catch(TskCoreException exception){
                logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
            }catch(Blackboard.BlackboardException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }
        
        logger.log(Level.INFO,"Method END: analyzeSearchItems");
    }
    
    private void analyzeStories(AppSQLiteDB msysDb, Content content){
        logger.log(Level.INFO, "Method start: analyzeStories");
        String query = "select story_id, author_id, timestamp_ms, text, media_url, media_playable_url from stories";
 
            try{
                ResultSet searchItemsSet = msysDb.runQuery(query);
                   
                if(searchItemsSet != null){
                    ArrayList<BlackboardArtifact> artifacts = new ArrayList<>();
                    ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                    while (searchItemsSet.next()){
                        String id = searchItemsSet.getString("story_id");
                        String author_id = searchItemsSet.getString("author_id");
                        long timestamp = searchItemsSet.getLong("timestamp_ms")/1000;
                        String text = searchItemsSet.getString("text");
                        String media_url = searchItemsSet.getString("media_url");
                        String media_playable_url = searchItemsSet.getString("media_playable_url");

                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_ID, moduleName, id));
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, moduleName, author_id));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT, moduleName, text != null ? text: ""));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, media_url != null ? media_url : ""));
                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, timestamp));

                        BlackboardAttribute.Type attributeType = currentCase.getSleuthkitCase().getAttributeType("MEDIA_PLAYABLE_URL");
                        if (attributeType == null) {
                                attributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("MEDIA_PLAYABLE_URL", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "media playable urle");
                        }
                        
                        attributes.add( new BlackboardAttribute(attributeType, moduleName, media_playable_url));
                        
                        BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                             "FB_STORY", "Facebook Story", BlackboardArtifact.Category.DATA_ARTIFACT);

                        BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                        artifacts.add(artifact);
                    
                    }
                     
                    blackboard.postArtifacts(artifacts, moduleName);   
                }
                
            }catch(SQLException exception){
                logger.log(Level.SEVERE, exception.getMessage() ,exception);
            }catch(TskDataException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }catch(TskCoreException exception){
                logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
            }catch(Blackboard.BlackboardException exception){
                logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
            }
        
        logger.log(Level.INFO,"Method END: analyzeStories");
    }
    
    private void extractOwnerFacebookId(AppSQLiteDB msysDb){
        logger.log(Level.INFO,"Method start: extractOwnerFacebookId");
        String query = "select id, facebook_user_id from _user_info";  
            try{
                ResultSet resultSet = msysDb.runQuery(query);
            
                if(resultSet != null){
                    
                     while (resultSet.next()){
                        ownerUserId = resultSet.getString("facebook_user_id");
                    }
                }          
            }catch(SQLException exception){
                logger.log(Level.WARNING, exception.getMessage() ,exception);
            }
        
        logger.log(Level.INFO,"Method end: extractOwnerFacebookId");
    }  
}
