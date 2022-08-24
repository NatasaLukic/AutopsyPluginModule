/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.ftn.messengerforensics;


import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import org.sleuthkit.autopsy.ingest.DataSourceIngestModule;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.openide.util.Exceptions;
import org.sleuthkit.autopsy.casemodule.Case;
import org.sleuthkit.autopsy.casemodule.NoCurrentCaseException;
import org.sleuthkit.autopsy.casemodule.services.FileManager;
import org.sleuthkit.autopsy.ingest.DataSourceIngestModuleProgress;
import org.sleuthkit.autopsy.ingest.IngestModule;

import org.sleuthkit.datamodel.TskCoreException;

import org.sleuthkit.autopsy.ingest.DataSourceIngestModule;
import org.sleuthkit.autopsy.ingest.IngestJobContext;
import org.sleuthkit.autopsy.ingest.IngestMessage;
import org.sleuthkit.autopsy.ingest.IngestModuleIngestJobSettings;
import org.sleuthkit.autopsy.ingest.IngestServices;

import org.sleuthkit.autopsy.coreutils.Logger;
import org.sleuthkit.autopsy.coreutils.AppSQLiteDB;
import org.sleuthkit.autopsy.coreutils.MessageNotifyUtil;

import org.sleuthkit.datamodel.*;
import org.sleuthkit.datamodel.BlackboardAttribute.Type;
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
    private IngestJobContext context;
    private final IngestServices services = IngestServices.getInstance();
    private Case currentCase;
    private FileManager fileManager;
    private Blackboard blackboard;
    private CommunicationArtifactsHelper communicationArtifactsHelper;
    
    private Logger logger = Logger.getLogger(MessengerDataSourceIngestModule.class.getName());;
    private static String FB_MESSENGER_PACKAGE_NAME = "com.facebook.orca";
    private static String MESSAGE_TYPE = "Facebook Messenger";
    private static String VERSION = "";
    private String ownerUserId = null;
    private CommunicationArtifactsHelper communicationArtifactsHelperAccounts = null;
    private CommunicationArtifactsHelper communicationArtifactsHelperThreads = null;

    public MessengerDataSourceIngestModule(IngestModuleIngestJobSettings settings){
        
    }
    
    @Override
    public void startUp(IngestJobContext context) throws IngestModuleException {
        this.context = context;
         
         try {
            currentCase = Case.getCurrentCaseThrows();
            fileManager = Case.getCurrentCaseThrows().getServices().getFileManager();
            blackboard = currentCase.getServices().getArtifactsBlackboard();
            
        } catch (NoCurrentCaseException ex) {
            logger.log(Level.SEVERE, "Exception while getting open case.", ex);
            throw new IngestModuleException("Exception while getting open case.", ex);
        }
    }

    @Override
    public ProcessResult process(Content content, DataSourceIngestModuleProgress dsimp) {
        dsimp.switchToIndeterminate();
        Collection<AppSQLiteDB> threadsDbs;
        Collection<AppSQLiteDB> searchCacheDbs;
        Collection<AppSQLiteDB> msysDbs;
        Collection<AppSQLiteDB> prefsDbs;
         
        try{
            prefsDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "sprefs_db", false, FB_MESSENGER_PACKAGE_NAME);
            threadsDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "threads_db2", false,FB_MESSENGER_PACKAGE_NAME);
            searchCacheDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "search_cache_db2", false,FB_MESSENGER_PACKAGE_NAME);
            msysDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "msys_database", false,FB_MESSENGER_PACKAGE_NAME);
            
            getAppVersion(prefsDbs, content);
            extractOwnerFacebookId(msysDbs);
            
            if(ownerUserId == null){
                communicationArtifactsHelperThreads  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(),"module name",content , Account.Type.FACEBOOK);
            }else{
                communicationArtifactsHelperThreads  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(),"module name",content , Account.Type.FACEBOOK,  Account.Type.FACEBOOK, ownerUserId);
            }
            analyzeContacts(msysDbs);
            analyzeMessages(threadsDbs, content);
            analyzeCallLogs(threadsDbs);
            analyzeSearchItems(searchCacheDbs, content);
            analyzeRecentSearchItems(searchCacheDbs, content);
            
            return IngestModule.ProcessResult.OK;
            
        }catch(Exception exception){
             logger.log(Level.SEVERE,exception.getMessage() ,exception);
             return IngestModule.ProcessResult.ERROR;
        }
    }

    @Override
    public void shutDown() {
        DataSourceIngestModule.super.shutDown();
    }
    private void getAppVersion(Collection<AppSQLiteDB> prefsDbs, Content content) throws TskCoreException{
        
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
    }
    
    public void analyzeContacts(Collection<AppSQLiteDB> msysDbs) {
        String query = "select id, name, profile_picture_url, phone_number, email_address, work_company_name, birthday_timestamp, username, "
                + "blocked_by_viewer_status, blocled_since_timestamp_ms from contacts";  
        for (AppSQLiteDB appSQLiteDB : msysDbs){
            
            try{
                ResultSet resultSet = appSQLiteDB.runQuery(query);
            
                if(resultSet != null){
                    
                     while (resultSet.next()){
                        String email = resultSet.getString("email_address")!= null ? resultSet.getString("email_address") : "";
                        String phoneNumber = resultSet.getString("phone_number") != null ? resultSet.getString("phone_number") : "";
                        String blockedBy = ""; 
                        if(resultSet.getInt("blocked_by_viewer_status") == 2){
                            blockedBy = "OWNER_USER";
                        }else if(resultSet.getInt("blocked_by_viewer_status") == 1){
                             blockedBy = "UNKNOWN";
                        }
                        
                        long blockedSince = resultSet.getLong("blocled_since_timestamp_ms") / 1000;
                        
                        ArrayList<BlackboardAttribute> additionalAttributes = new ArrayList<>();
       
                        additionalAttributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, "module_Name", resultSet.getString("id")));  
                        additionalAttributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME, "module_Name", resultSet.getString("username")));
                        additionalAttributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, "module_name", resultSet.getString("profile_picture_url")));
                        additionalAttributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, "module_Name", blockedSince));
                        
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
                        
                        additionalAttributes.add( new BlackboardAttribute(workCompanyAttributeType, "module_name", resultSet.getString("work_company_name")));
                        additionalAttributes.add( new BlackboardAttribute(blockedAttributeType, "module_name", blockedBy));
                        
                        
                         communicationArtifactsHelperAccounts.addContact(resultSet.getString("name"), 
                                                    phoneNumber,
                                                    "", 
                                                    "",
                                                    email,	       
                                                    additionalAttributes);
                        
                    }
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
            finally{
                appSQLiteDB.close();
            }
        }
    }
    
    public void analyzeRecentSearchItems(Collection<AppSQLiteDB> searchCacheDbs, Content content) {

        String query = "select fbid, item_type, display_name, first_name, last_name, picture_url, most_recent_pick_time_ms, total_pick_count from recent_search_items";
        
        for (AppSQLiteDB appSQLiteDB : searchCacheDbs){
            
            try{
                ResultSet searchItemsSet = appSQLiteDB.runQuery(query);
            
                if(searchItemsSet != null){
                     ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                     while (searchItemsSet.next()){
                        String fbid = searchItemsSet.getString("fbid");
                        String itemType = searchItemsSet.getString("item_type");
                        String displayName = searchItemsSet.getString("display_name");
                        String firstName = searchItemsSet.getString("first_name");
                        String lastname = searchItemsSet.getString("last_name");
                        String pictureUrl = searchItemsSet.getString("picture_url");

                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_ID, "module_Name", fbid));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, "module_name",displayName + " - " + firstName + " " +lastname));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, "module_name", pictureUrl));

                        BlackboardAttribute.Type itemTypeAttributeType = currentCase.getSleuthkitCase().getAttributeType("ITEM_TYPE");
                        if (itemTypeAttributeType == null) {
                                itemTypeAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("ITEM_TYPE", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Item type");
                        }
                        
                        attributes.add( new BlackboardAttribute(itemTypeAttributeType, "module_name", itemType));
                        
                    }
                     
                     //TODO: Add attributes for most_recent_pick_time_ms, total_pick_count
                    BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                             "RECENT_SEARCH_ITEMS", "Recent Search Items", BlackboardArtifact.Category.DATA_ARTIFACT);

                    BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                    blackboard.postArtifact(artifact, "module_name");
                     
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
            finally{
                appSQLiteDB.close();
            }
        }
           
          
    }
    
    public CommunicationDirection findCommunicationDirection(String senderId){
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
        String senderId = "";
        if(json != null && json != ""){
            Pattern pattern = Pattern.compile("(?i).*\\\"user.key\\\":\\\"FACEBOOK:(\\d+)\\\"");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderId = matcher.group(1);
            }
         }
        return senderId;
    }
    
    private String extractSenderName(String json){
        String senderName = "";
        if(json != null && json != ""){
            Pattern pattern = Pattern.compile("(?i)\\\"name\\\":\\\"(\\w+\\s*\\w*)\\\"");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderName = matcher.group(1);
            }
         }
        return senderName;
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
    
    private void analyzeMessages(Collection<AppSQLiteDB> threadsDbs, Content content) throws TskCoreException, Blackboard.BlackboardException{
            for (AppSQLiteDB appSQLiteDB : threadsDbs) {

                try{

                     if(ownerUserId == null){
                        communicationArtifactsHelperThreads  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(),"module name",content , Account.Type.FACEBOOK);
                     }else{
                         communicationArtifactsHelperThreads  = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(),"module name",content , Account.Type.FACEBOOK,  Account.Type.FACEBOOK, ownerUserId);
                     }
                         
                     String query = "SELECT msg_id, text, sender, datetime(timestamp_ms, 'unixepoch') 'timestamp', msg_type, messages.thread_key as thread_key," +
                                    "snippet, thread_participants.user_key as user_key, thread_users.name as name," +
                                    "attachments, pending_send_media_attachment FROM messages" +
                                    "JOIN thread_participants ON messages.thread_key = thread_participants.thread_key" +
                                    "JOIN thread_users ON thread_participants.user_key = thread_users.user_key " +
                                    "WHERE msg_type not in (9, 203) ORDER BY msg_id";
                    ResultSet threadsResultSet = appSQLiteDB.runQuery(query);
                     
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
                    
                    while (threadsResultSet.next()) {
                        String msgId = threadsResultSet.getString("msg_id");

                        if(msgId != oldMsgId){

                            if(oldMsgId != null){
                                BlackboardArtifact messageArtifact = communicationArtifactsHelperThreads.addMessage(MESSAGE_TYPE, direction, fromId, recipientIdsList,timeStamp,
                                                                     CommunicationArtifactsHelper.MessageReadStatus.UNKNOWN, "", msgText, threadId);

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
                            String text = threadsResultSet.getString("text").trim() == "" ? threadsResultSet.getString("snippet") : threadsResultSet.getString("text");
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
                    
                    communicationArtifactsHelperThreads.addMessage(MESSAGE_TYPE, direction, fromId, recipientIdsList,timeStamp,
                                                                   CommunicationArtifactsHelper.MessageReadStatus.UNKNOWN, "", msgText, threadId);

                      
                }catch(SQLException exception){
                    logger.log(Level.SEVERE, exception.getMessage() ,exception);
                }catch(TskCoreException exception){
                    logger.log(Level.SEVERE, "Failed to add FB Messenger message artifacts." ,exception);
                }catch(Blackboard.BlackboardException exception){
                    logger.log(Level.SEVERE, "Failed to post artifacts." ,exception);
                }finally{
                        appSQLiteDB.close();
                    }    
                }
    }
    
    private void analyzeCallLogs(Collection<AppSQLiteDB> threadsDbs){
        /*
        msg_type indicates type of call:
            9: one to one calls
            203: group call
        1-to-1 calls only have a call_ended record.
        group calls have a call_started_record as well as call_ended recorded, with *different* message ids.
        all the data we need can be found in the call_ended record.
        */
            
        String query = "SELECT msg_id, text, sender, timestamp_ms, msg_type, admin_text_thread_rtc_event, generic_admin_message_extensible_data, "+
                       "messages.thread_key as thread_key, thread_participants.user_key as user_key, thread_users.name as name " +
                       "FROM messages JOIN thread_participants ON messages.thread_key = thread_participants.thread_key "+
                       "JOIN thread_users ON thread_participants.user_key = thread_users.user_key WHERE msg_type = 9 OR (msg_type = 203 AND admin_text_thread_rtc_event = 'group_call_ended') " +
                       "ORDER BY msg_id";
        
        for (AppSQLiteDB appSQLiteDB : threadsDbs){
            
            try{
                            ResultSet messagesResultSet = appSQLiteDB.runQuery(query);
            
            if(messagesResultSet != null){
                String oldMsgId = null;
                CommunicationDirection direction = CommunicationDirection.UNKNOWN;
                String callerId = null;
                String senderName = null;
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
                                                        mediaType);

                            
                        }

                        oldMsgId = msgId;
                        
                        callerId = extractSenderId(messagesResultSet.getString("sender"));
                        senderName = extractSenderName(messagesResultSet.getString("sender"));

                        direction = findCommunicationDirection(callerId);
                        endTimeStamp = messagesResultSet.getLong("timestamp_ms") / 1000;
                        
                        String recepientId = extractRecepientId(messagesResultSet.getString("user_key"), callerId);
                        if(callerId != recepientId){
                            calleeIdsList.add(recepientId);
                        }
                        
                        String json = messagesResultSet.getString("generic_admin_message_extensible_data");
                        if(json != null && json.trim() != ""){
                            Pattern pattern = Pattern.compile("\\\"call_duration\\\":(\\d+),");
                            Matcher matcher = pattern.matcher(json);
                            if (matcher.matches()) {
                                duration = Integer.parseInt(matcher.group(1));
                            }
                            
                            pattern = Pattern.compile("\\\"video\\\":(\\w+),");
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
                 
                 BlackboardArtifact messageArtifact = communicationArtifactsHelperThreads.addCalllog( 
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
            finally{
                appSQLiteDB.close();
            }

        
        }
 
    }
    
    private void analyzeSearchItems(Collection<AppSQLiteDB> searchCacheDbs, Content content) throws TskDataException{
        
        String query = "select fbid, item_type, display_name, first_name, last_name, picture_url from search_items";
        
        for (AppSQLiteDB appSQLiteDB : searchCacheDbs){
            
            try{
                ResultSet searchItemsSet = appSQLiteDB.runQuery(query);
            
                if(searchItemsSet != null){
                     ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                     while (searchItemsSet.next()){
                        String fbid = searchItemsSet.getString("fbid");
                        String itemType = searchItemsSet.getString("item_type");
                        String displayName = searchItemsSet.getString("display_name");
                        String firstName = searchItemsSet.getString("first_name");
                        String lastname = searchItemsSet.getString("last_name");
                        String pictureUrl = searchItemsSet.getString("picture_url");

                        attributes.add( new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_ID, "module_Name", fbid));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, "module_name",displayName + " - " + firstName + " " +lastname));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, "module_name", pictureUrl));

                        BlackboardAttribute.Type itemTypeAttributeType = currentCase.getSleuthkitCase().getAttributeType("ITEM_TYPE");
                        if (itemTypeAttributeType == null) {
                                itemTypeAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("ITEM_TYPE", 
                                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Item type");
                        }
                        
                        attributes.add( new BlackboardAttribute(itemTypeAttributeType, "module_name", itemType));
                        
                    }
                     
                     
                    BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                             "SEARCH_ITEMS", "Search Items", BlackboardArtifact.Category.DATA_ARTIFACT);

                    BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                    blackboard.postArtifact(artifact, "module_name");
                     
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
            finally{
                appSQLiteDB.close();
            }
        }
    }
    
    private void analyzeStories(Collection<AppSQLiteDB> msysDbs){
        
    }
    
    private void extractOwnerFacebookId(Collection<AppSQLiteDB> msysDbs){
        String query = "select id, facebook_user_id from _user_info";  
        for (AppSQLiteDB appSQLiteDB : msysDbs){
            
            try{
                ResultSet resultSet = appSQLiteDB.runQuery(query);
            
                if(resultSet != null){
                    
                     while (resultSet.next()){
                        ownerUserId = resultSet.getString("facebook_user_id");
                    }
                }          
            }catch(SQLException exception){
                logger.log(Level.WARNING, exception.getMessage() ,exception);
            }finally{
                appSQLiteDB.close();
            }
        }
    }  
}
