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
public class MessengerDataSourceIngestModule implements DataSourceIngestModule {

    private static final String moduleName = MessengerIngestModuleFactory.getModuleName();
    private IngestJobContext context;
    private Case currentCase;
    private Blackboard blackboard;

    private Logger logger = Logger.getLogger(MessengerIngestModuleFactory.getModuleName());
    private static String FB_MESSENGER_PACKAGE_NAME = "com.facebook.orca";
    private static String MESSAGE_TYPE = "Facebook Messenger";
    private static String VERSION = "";
    private String ownerUserId = null;
    private CommunicationArtifactsHelper communicationArtifactsHelperThreads = null;

    public MessengerDataSourceIngestModule() {

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
        logger.log(Level.INFO, "Started proccessing...");
        dsimp.switchToIndeterminate();
        Collection<AppSQLiteDB> threadsDbs;
        Collection<AppSQLiteDB> searchCacheDbs;
        Collection<AppSQLiteDB> msysDbs;
        Collection<AppSQLiteDB> prefsDbs;

        try {

            prefsDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "prefs_db", true, FB_MESSENGER_PACKAGE_NAME);
            threadsDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "threads_db2", true, FB_MESSENGER_PACKAGE_NAME);
            searchCacheDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "search_cache_db", true, FB_MESSENGER_PACKAGE_NAME);
            msysDbs = AppSQLiteDB.findAppDatabases((DataSource) content, "msys_database", false, FB_MESSENGER_PACKAGE_NAME);
            logger.log(Level.INFO, " prefsDbs count: " + prefsDbs.size());
            logger.log(Level.INFO, " threadsDbs count: " + threadsDbs.size());
            logger.log(Level.INFO, " searchCacheDbs count: " + searchCacheDbs.size());
            logger.log(Level.INFO, " msysDbs count " + msysDbs.size());

            getAppVersion(prefsDbs, content);
            logger.log(Level.INFO, "App Version: " + VERSION);
            
            ArrayList<BlackboardAttribute> attributes = new ArrayList<>();
            attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_VERSION, moduleName, VERSION));
            BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                            "FACEBOOK_MESSENGER_APPLICATION_VERSION", "Version of Application", BlackboardArtifact.Category.DATA_ARTIFACT);
            BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
            blackboard.postArtifact(artifact, moduleName);
            

            for (AppSQLiteDB msysDb : msysDbs) {
                logger.log(Level.INFO, " DbFilename: " + msysDb.getDBFile().getName());

                try {
                    extractOwnerFacebookId(msysDb);

                    analyzeContacts(msysDb, content);
                    analyzeStories(msysDb, content);

                } catch (Exception exception) {
                    logger.log(Level.WARNING, exception.getMessage(), exception);
                } finally {
                    msysDb.close();
                }
            }

            for (AppSQLiteDB threadsDb : threadsDbs) {
                logger.log(Level.INFO, " DbFilename: " + threadsDb.getDBFile().getName());
                try {
                    if (communicationArtifactsHelperThreads == null) {
                        if (ownerUserId == null) {
                            communicationArtifactsHelperThreads = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(), moduleName, content, Account.Type.FACEBOOK);
                        } else {
                            communicationArtifactsHelperThreads = new CommunicationArtifactsHelper(currentCase.getSleuthkitCase(), moduleName, content, Account.Type.FACEBOOK, Account.Type.FACEBOOK, ownerUserId);
                        }
                    }
                    analyzeCallLogs(threadsDb);
                    analyzeMessages(threadsDb, content);

                } catch (Exception exception) {
                    logger.log(Level.WARNING, exception.getMessage(), exception);
                } finally {
                    threadsDb.close();
                }
            }

            for (AppSQLiteDB searchCacheDb : searchCacheDbs) {
                logger.log(Level.INFO, " DbFilename: " + searchCacheDb.getDBFile().getName());
                try {
                    analyzeSearchItems(searchCacheDb, content);
                    analyzeRecentSearchItems(searchCacheDb, content);
                } catch (Exception exception) {
                    logger.log(Level.WARNING, exception.getMessage(), exception);
                } finally {
                    searchCacheDb.close();
                }
            }

            logger.log(Level.INFO, "Finished proccessing...");
            return IngestModule.ProcessResult.OK;

        } catch (Exception exception) {
            logger.log(Level.SEVERE, exception.getMessage(), exception);
            return IngestModule.ProcessResult.ERROR;
        }
    }

    private void getAppVersion(Collection<AppSQLiteDB> prefsDbs, Content content) throws TskCoreException {
        String query = "SELECT key, type, value FROM preferences WHERE key LIKE '%app_version_name_current%'";

        for (AppSQLiteDB appSQLiteDB : prefsDbs) {
            try {
                ResultSet resultSet = appSQLiteDB.runQuery(query);
                if (resultSet != null) {
                    while (resultSet.next()) {
                        VERSION = resultSet.getString("value");
                    }
                }

            } catch (SQLException exception) {
                logger.log(Level.WARNING, exception.getMessage(), exception);
            } finally {
                appSQLiteDB.close();
            }
        }
    }

    private void analyzeContacts(AppSQLiteDB msysDb, Content content) {
        logger.log(Level.INFO, "Method start: analyzeContacts");
        String query = "select id, name, profile_picture_url, phone_number, email_address, work_company_name, birthday_timestamp, username, friendship_status, blocked_by_viewer_status, blocked_since_timestamp_ms from contacts";

        try {
            ResultSet resultSet = msysDb.runQuery(query);

            if (resultSet != null) {
                Collection<BlackboardArtifact> artifacts = new ArrayList<>();
                while (resultSet.next()) {
                    String email = resultSet.getString("email_address") != null ? resultSet.getString("email_address") : "";
                    String phoneNumber = resultSet.getString("phone_number") != null ? resultSet.getString("phone_number") : "";
                    String blockedBy = "";
                    if (resultSet.getInt("blocked_by_viewer_status") == 2) {
                        blockedBy = "OWNER_USER";
                    } else if (resultSet.getInt("blocked_by_viewer_status") == 1) {
                        blockedBy = "UNKNOWN";
                    }

                    int friendshipStatus = resultSet.getInt("friendship_status");
                    long blockedSince = resultSet.getLong("blocked_since_timestamp_ms") / 1000;

                    ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, moduleName, resultSet.getString("id")));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME, moduleName, resultSet.getString("name")));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME, moduleName, resultSet.getString("username")));
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

                    BlackboardAttribute.Type friendshipStatusAttrType = currentCase.getSleuthkitCase().getAttributeType("FB_FRIENDSHIP_STATUS");
                    if (friendshipStatusAttrType == null) {
                        friendshipStatusAttrType = currentCase.getSleuthkitCase().addArtifactAttributeType("FB_FRIENDSHIP_STATUS",
                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "FB Friendship Status");
                    }

                    attributes.add(new BlackboardAttribute(workCompanyAttributeType, moduleName, resultSet.getString("work_company_name") != null ? resultSet.getString("work_company_name") : ""));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, blockedSince));
                    attributes.add(new BlackboardAttribute(blockedAttributeType, moduleName, blockedBy));
                    attributes.add(new BlackboardAttribute(friendshipStatusAttrType, moduleName, friendshipStatus == 1 ? "Friends" : ""));

                    BlackboardArtifact artifact = content.newDataArtifact(BlackboardArtifact.Type.TSK_CONTACT, attributes);
                    artifacts.add(artifact);
                }

                blackboard.postArtifacts(artifacts, moduleName);
            }
        } catch (SQLException exception) {
            logger.log(Level.WARNING, exception.getMessage(), exception);
        } catch (TskCoreException exception) {
            logger.log(Level.WARNING, exception.getMessage(), exception);
        } catch (TskDataException exception) {
            logger.log(Level.WARNING, exception.getMessage(), exception);
        } catch (Blackboard.BlackboardException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        }
        logger.log(Level.INFO, "Method end: analyzeContacts");
    }

    private void analyzeRecentSearchItems(AppSQLiteDB searchCacheDb, Content content) {
        logger.log(Level.INFO, "Method start: analyzeRecentSearchItems");
        String query = "select fbid, item_type, display_name, first_name, last_name, picture_url from recent_search_items";

        try {
            ResultSet searchItemsSet = searchCacheDb.runQuery(query);

            if (searchItemsSet != null) {
                ArrayList<BlackboardArtifact> artifacts = new ArrayList<>();
                ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                while (searchItemsSet.next()) {
                    String fbid = searchItemsSet.getString("fbid");
                    String itemType = searchItemsSet.getString("item_type");
                    String displayName = searchItemsSet.getString("display_name");
                    
                    String pictureUrl = searchItemsSet.getString("picture_url");

                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, moduleName, fbid));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, displayName));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, pictureUrl));

                    BlackboardAttribute.Type itemTypeAttributeType = currentCase.getSleuthkitCase().getAttributeType("ITEM_TYPE");
                    if (itemTypeAttributeType == null) {
                        itemTypeAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("ITEM_TYPE",
                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Item type");
                    }

                    attributes.add(new BlackboardAttribute(itemTypeAttributeType, moduleName, itemType));

                    BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                            "RECENT_SEARCH_ITEMS", "Recent Search Items", BlackboardArtifact.Category.DATA_ARTIFACT);

                    BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                    artifacts.add(artifact);

                }

                blackboard.postArtifacts(artifacts, moduleName);

            }

        } catch (SQLException exception) {
            logger.log(Level.SEVERE, exception.getMessage(), exception);
        } catch (TskDataException | Blackboard.BlackboardException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        } catch (TskCoreException exception) {
            logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
        }

        logger.log(Level.INFO, "Method end: analyzeRecentSearchItems");
    }

    private CommunicationDirection findCommunicationDirection(String senderId) {
        CommunicationDirection direction = CommunicationDirection.UNKNOWN;
        if (senderId != null) {
            if (!senderId.equals(ownerUserId)) {
                direction = CommunicationDirection.INCOMING;
            } else {
                direction = CommunicationDirection.OUTGOING;
            }
        }

        return direction;
    }

    private String extractSenderId(String json) {
        logger.log(Level.INFO, json);
        String senderId = "";
        if (json != null && !"".equals(json)) {

            Pattern pattern = Pattern.compile(".*FACEBOOK:(\\d+).*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderId = matcher.group(1);
            }
        }
        return senderId;
    }

    private String extractSenderEmail(String json) {
        String senderEmail = "";
        if (json != null && json != "") {

            Pattern pattern = Pattern.compile(".*email\":\"?(.*?)\"?,\".*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderEmail = matcher.group(1);
            }
        }
        return senderEmail;
    }

    private String extractSenderName(String json) {
        String senderName = "";
        if (json != null && !"".equals(json)) {
            Pattern pattern = Pattern.compile(".*name\":\"?(.*?)\"?,\".*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderName = matcher.group(1);
            }
        }
        return senderName;
    }

    private String extractSenderPhone(String json) {
        String senderPhone = "";
        if (json != null && !"".equals(json)) {
            Pattern pattern = Pattern.compile(".*phone\":\"?(.*?)\"?,\".*");
            Matcher matcher = pattern.matcher(json);
            if (matcher.matches()) {
                senderPhone = matcher.group(1);
            }
        }
        return senderPhone;
    }

    private String extractRecepientId(String user_key) {
        String recepient = "";
        if (user_key != null && !"".equals(user_key)) {
            Pattern pattern = Pattern.compile("(?i)\\w+:(\\d+)");
            Matcher matcher = pattern.matcher(user_key);
            if (matcher.matches()) {
                recepient = matcher.group(1);
            }
        }

        return recepient;
    }

    private void analyzeMessages(AppSQLiteDB threadsDb, Content content) throws TskCoreException, Blackboard.BlackboardException {
        logger.log(Level.INFO, "Method start: analyzeMessages");
        try {

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
            ArrayList<BlackboardAttribute> additionalAttributes = new ArrayList<>();

            while (threadsResultSet.next()) {
                String msgId = threadsResultSet.getString("msg_id");

                if (!msgId.equals(oldMsgId)) {

                    if (oldMsgId != null) {
                        BlackboardArtifact messageArtifact = communicationArtifactsHelperThreads.addMessage(MESSAGE_TYPE, direction, fromId, recipientIdsList, timeStamp,
                                CommunicationArtifactsHelper.MessageReadStatus.UNKNOWN, "", msgText, threadId, additionalAttributes);
                        additionalAttributes = new ArrayList<>();
                        recipientIdsList = new ArrayList<>();
                        if (messageAttachments != null) {
                            communicationArtifactsHelperThreads.addAttachments(messageArtifact, messageAttachments);
                            messageAttachments = null;
                        }
                    }

                    oldMsgId = msgId;
                    threadId = threadsResultSet.getString("thread_key");
                    timeStamp = threadsResultSet.getLong("timestamp_ms") / 1000;
                    fromId = extractSenderId(threadsResultSet.getString("sender"));
                    senderName = extractSenderName(threadsResultSet.getString("sender"));

                    direction = findCommunicationDirection(fromId);
                    msgText = threadsResultSet.getString("text") != null ? threadsResultSet.getString("text") : "";
                    snnipet = threadsResultSet.getString("snippet") != null ? threadsResultSet.getString("snippet") : "";
                    shares = threadsResultSet.getString("shares") != null ? threadsResultSet.getString("shares") : "";

                    BlackboardAttribute.Type attributeType = currentCase.getSleuthkitCase().getAttributeType("SHARED_ITEM");
                    if (attributeType == null) {
                        attributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("SHARED_ITEM",
                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Shared item");
                    }

                    additionalAttributes.add(new BlackboardAttribute(attributeType, moduleName, shares));

                    attachment = threadsResultSet.getString("attachments");
                    pendingSendMediaAttachment = threadsResultSet.getString("pending_send_media_attachment");

                    List<URLAttachment> urlAttachments = new ArrayList<>();
                    List<FileAttachment> fileAttachments = new ArrayList<>();
                    
                    if (attachment != null || pendingSendMediaAttachment != null) {
                        if (attachment != null) {
                            Pattern pattern = Pattern.compile("(?i).*\"mime_type\":\"(\\w+/\\w*)\",.*");
                            Matcher matcher = pattern.matcher(attachment);
                            if (matcher.matches()) {
                                String mimeType = matcher.group(1);
                                if (null == mimeType) {
                                    logger.log(Level.WARNING, "Attachment type not supported: {0}", mimeType);
                                } else {
                                    switch (mimeType) {
                                        case "image/jpeg":
                                        case "image/gif":
                                            Pattern srcPattern = Pattern.compile("(?i)(https?[-a-zA-Z\\d@:%._\\\\+~#=&?/]+)\\\\{3}\"");
                                            matcher = srcPattern.matcher(attachment);
                                            
                                            while (matcher.find()) {
                                                for (int i = 1; i <= matcher.groupCount(); i++) {
                                                    logger.log(Level.INFO, "Group " + i + ": " + matcher.group(i));
                                                     urlAttachments.add(new URLAttachment(matcher.group(i)));
                                                }
                                            }

                                            break;
                                        case "video/mp4": {
                                            Pattern videoUrlPattern = Pattern.compile("(?i)\"video_data_url\":\"(https?[-a-zA-Z\\d@:%._\\\\+~#=&?/]+)\"");
                                            matcher = videoUrlPattern.matcher(attachment);
                                            while (matcher.find()) {
                                                for (int i = 1; i <= matcher.groupCount(); i++) {
                                                    logger.log(Level.INFO, "Group " + i + ": " + matcher.group(i));
                                                     urlAttachments.add(new URLAttachment(matcher.group(i)));
                                                }
                                            }
                                           
                                            videoUrlPattern = Pattern.compile("(?i)\"video_data_thumbnail_url\":\"(https?[-a-zA-Z\\d@:%._\\\\+~#=&?/]+)\"");
                                            matcher = videoUrlPattern.matcher(attachment);
                                            while (matcher.find()) {
                                                for (int i = 1; i <= matcher.groupCount(); i++) {
                                                    logger.log(Level.INFO, "Group " + i + ": " + matcher.group(i));
                                                    urlAttachments.add(new URLAttachment(matcher.group(i)));
                                                }
                                            }
                                            break;
                                        }
                                        case "audio/mp4":
                                        case "audio/mpeg": {
                                            Pattern videoUrlPattern = Pattern.compile("(?i)\"audio_uri\":\"(https?[-a-zA-Z\\d@:%._\\\\+~#=&?/]+)\"");
                                            matcher = videoUrlPattern.matcher(attachment);
                                           while (matcher.find()) {
                                                for (int i = 1; i <= matcher.groupCount(); i++) {
                                                    logger.log(Level.INFO, "Group " + i + ": " + matcher.group(i));
                                                    urlAttachments.add(new URLAttachment(matcher.group(i)));
                                                }
                                            }
                                            break;
                                        }
                                        case "text/plain": {
                                            //[{"id":"343651987283990","fbid":"343651987283990","mime_type":"text/plain","filename":"Program.cs","file_size":1542}]
                                           
                                           
                                            break;
                                        }
                                        default:
                                            logger.log(Level.WARNING, "Attachment type not supported: {0}", mimeType);
                                            break;
                                    }
                                }

                            }
                        }
                        if (pendingSendMediaAttachment != null) {
                            Pattern uriPattern = Pattern.compile("(?i)\"uri\":\"([-a-zA-Z\\d@:%._\\\\+~#=&?/]+)\"");
                            Matcher matcher = uriPattern.matcher(attachment);

                            while (matcher.find()) {
                                for (int i = 1; i <= matcher.groupCount(); i++) {
                                    String attachmentUri = matcher.group(i).replace("file://", "");
                                    fileAttachments.add(new FileAttachment(currentCase.getSleuthkitCase(), content, attachmentUri));
                                }
                            }
                        }

                        messageAttachments = new MessageAttachments(fileAttachments, urlAttachments);

                    }

                    String recepientId = extractRecepientId(threadsResultSet.getString("user_key"));
                    if (!fromId.equals(recepientId)) {
                        recipientIdsList.add(recepientId);
                    }

                } else { // same msgId as previous, just collect recipient
                    String recepientId = extractRecepientId(threadsResultSet.getString("user_key"));
                    if (!fromId.equals(recepientId)) {
                        recipientIdsList.add(recepientId);
                    }
                }

            }

            communicationArtifactsHelperThreads.addMessage(MESSAGE_TYPE, direction, fromId, recipientIdsList, timeStamp,
                    CommunicationArtifactsHelper.MessageReadStatus.UNKNOWN, "", msgText, threadId, additionalAttributes);

        } catch (SQLException exception) {
            logger.log(Level.SEVERE, exception.getMessage(), exception);
        } catch (TskCoreException exception) {
            logger.log(Level.SEVERE, "Failed to add FB Messenger message artifacts.", exception);
        } catch (Blackboard.BlackboardException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        } catch (TskDataException exception) {
            logger.log(Level.SEVERE, "Failed to add blackboard attribute.", exception);
        }

        logger.log(Level.INFO, "Method end: analyzeMessages");

    }

    private void analyzeCallLogs(AppSQLiteDB threadsDb) {
        logger.log(Level.INFO, "Method START: analyzeCallLogs");
        /*
        msg_type indicates type of call:
            9: one to one calls
            203: group call
        1-to-1 calls only have a call_ended record.
        group calls have a call_started_record as well as call_ended recorded, with *different* message ids.
        all the data we need can be found in the call_ended record.
         */
        String query = "SELECT msg_id, text, sender, timestamp_ms, msg_type, admin_text_thread_rtc_event, generic_admin_message_extensible_data, messages.thread_key as thread_key, thread_participants.user_key as user_key, thread_users.name as name FROM messages JOIN thread_participants ON messages.thread_key = thread_participants.thread_key JOIN thread_users ON thread_participants.user_key = thread_users.user_key WHERE msg_type = 9 OR (msg_type = 203 AND admin_text_thread_rtc_event = 'group_call_ended') ORDER BY msg_id";

        try {
            ResultSet messagesResultSet = threadsDb.runQuery(query);

            if (messagesResultSet != null) {

                ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                String oldMsgId = null;
                CommunicationDirection direction = CommunicationDirection.UNKNOWN;
                String callerId = null;
                ArrayList<String> calleeIdsList = new ArrayList<>();
                long startTimeStamp = -1;
                long endTimeStamp = -1;
                int duration = 0; // call duration in seconds
                CallMediaType mediaType = CallMediaType.AUDIO;

                while (messagesResultSet.next()) {
                    String msgId = messagesResultSet.getString("msg_id");
                    logger.log(Level.INFO, "RAZLICITA PORUKA\t msgId: {0}\toldMsgId: {1}", new Object[]{msgId, oldMsgId});
                    if (!msgId.equals(oldMsgId)) {
                        if (oldMsgId != null) {
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

                        String recepientId = extractRecepientId(messagesResultSet.getString("user_key"));
                        if (!callerId.equals(recepientId)) {
                            calleeIdsList.add(recepientId);
                            logger.log(Level.INFO, "recepientId: " + recepientId + "\tcallerId: " + callerId);
                        }
                        String senderName = extractSenderName(messagesResultSet.getString("sender"));
                        String senderEmail = extractSenderEmail(messagesResultSet.getString("sender"));
                        String senderPhone = extractSenderPhone(messagesResultSet.getString("sender"));

                        //logger.log(Level.INFO,"msgId: " + msgId);
                        logger.log(Level.INFO, "callerId: " + callerId);
                        logger.log(Level.INFO, "direction: " + direction);
                        logger.log(Level.INFO, "recepientId: " + recepientId);
                        logger.log(Level.INFO, "senderName: " + senderName);
                        logger.log(Level.INFO, "senderEmail: " + senderEmail);
                        logger.log(Level.INFO, "senderPhone: " + senderPhone);

                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_EMAIL_FROM, moduleName, senderEmail));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, senderName));
                        attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM, moduleName, senderPhone));

                        String json = messagesResultSet.getString("generic_admin_message_extensible_data");
                        if (json != null && json.trim() != "") {
                            Pattern pattern = Pattern.compile("\"call_duration\":(\\d+),");
                            Matcher matcher = pattern.matcher(json);
                            if (matcher.matches()) {
                                duration = Integer.parseInt(matcher.group(1));
                            }

                            pattern = Pattern.compile("\"video\":(\\w+),");
                            matcher = pattern.matcher(json);
                            if (matcher.matches()) {
                                mediaType = "true".equals(matcher.group(1).toLowerCase()) ? CallMediaType.VIDEO : CallMediaType.AUDIO;
                            }
                        }

                        startTimeStamp = endTimeStamp - duration;

                    } else {
                        logger.log(Level.INFO, "ISTA PORUKA\t msgId: " + msgId + "\toldMsgId: " + oldMsgId);
                        String recepientId = extractRecepientId(messagesResultSet.getString("user_key"));

                        if (!callerId.equals(recepientId)) {
                            calleeIdsList.add(recepientId);
                            logger.log(Level.INFO, "recepientId: " + recepientId + "\tcallerId: " + callerId);
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

        } catch (SQLException exception) {
            logger.log(Level.SEVERE, exception.getMessage(), exception);
        } catch (TskCoreException exception) {
            logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
        } catch (Blackboard.BlackboardException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        }

        logger.log(Level.INFO, "Method end: analyzeCallLogs");

    }

    private void analyzeSearchItems(AppSQLiteDB searchCacheDb, Content content) throws TskDataException {
        logger.log(Level.INFO, "Method start: analyzeSearchItems");
        String query = "select fbid, item_type, display_name, first_name, last_name, picture_url from search_items";

        try {
            ResultSet searchItemsSet = searchCacheDb.runQuery(query);

            if (searchItemsSet != null) {
                ArrayList<BlackboardArtifact> artifacts = new ArrayList<>();
                ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                while (searchItemsSet.next()) {
                    String fbid = searchItemsSet.getString("fbid");
                    String itemType = searchItemsSet.getString("item_type");
                    String displayName = searchItemsSet.getString("display_name");
                    String pictureUrl = searchItemsSet.getString("picture_url");

                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_ID, moduleName, fbid));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, displayName));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, pictureUrl));

                    BlackboardAttribute.Type itemTypeAttributeType = currentCase.getSleuthkitCase().getAttributeType("ITEM_TYPE");
                    if (itemTypeAttributeType == null) {
                        itemTypeAttributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("ITEM_TYPE",
                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Item type");
                    }

                    attributes.add(new BlackboardAttribute(itemTypeAttributeType, moduleName, itemType));
                    BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                            "SEARCH_ITEMS", "Search Items", BlackboardArtifact.Category.DATA_ARTIFACT);

                    BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                    artifacts.add(artifact);
                }

                blackboard.postArtifacts(artifacts, moduleName);

            }

        } catch (SQLException exception) {
            logger.log(Level.SEVERE, exception.getMessage(), exception);
        } catch (TskDataException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        } catch (TskCoreException exception) {
            logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
        } catch (Blackboard.BlackboardException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        }

        logger.log(Level.INFO, "Method END: analyzeSearchItems");
    }
    
    private void analyzeStories(AppSQLiteDB msysDb, Content content) {
        logger.log(Level.INFO, "Method start: analyzeStories");
        String query = "select story_id, author_id, timestamp_ms, text, media_url, media_playable_url from stories";

        String bucketStoriesQuery = "SELECT B.bucket_id, B.owner_id, B.bucket_type, B.bucket_name, S.story_id, S.posting_status, S.type, S.optimistic_client_id, S.composer_session_id, S.author_id, S.timestamp_ms, S.ttl_ms, S.timestamp_ms + S.ttl_ms AS story_expiration_timestamp_ms, S.text, S.sticker_id, S.feed_encoded_id, S.media_id, S.media_type, S.media_url, S.media_url_expiration_timestamp_ms, S.media_fallback_url, S.media_width, S.media_height, S.media_playable_url, S.media_playable_url_expiration_timestamp_ms, S.media_playable_fallback_url, S.media_preview_url, S.media_preview_fallback_url, S.media_preview_url_expiration_timestamp_ms, S.media_preview_width, S.media_preview_height, S.media_thumbnail_url, S.media_thumbnail_fallback_url, S.media_thumbnail_url_expiration_timestamp_ms, S.media_thumbnail_width, S.media_thumbnail_height, S.background_gradient_color_top, S.background_gradient_color_bottom, S.caption_font_color, S.media_dash_manifest, S.attribution_text, S.authority_level IS 20 AS is_being_posted, S.authority_level IS 100 AS is_being_deleted, (julianday('now') - 2440587.5) * 86400000 - S.timestamp_ms > S.ttl_ms AS is_expired, (NOT RS.is_read) IS 0 AS is_read  FROM stories AS S INNER JOIN story_buckets AS B ON B.bucket_id = S.bucket_id LEFT OUTER JOIN read_stories AS RS ON RS.story_id = S.story_id";
        try {
            ResultSet searchItemsSet = msysDb.runQuery(query);

            if (searchItemsSet != null) {
                ArrayList<BlackboardArtifact> artifacts = new ArrayList<>();
                ArrayList<BlackboardAttribute> attributes = new ArrayList<>();

                while (searchItemsSet.next()) {
                    String id = searchItemsSet.getString("story_id");
                    String author_id = searchItemsSet.getString("author_id");
                    long timestamp = searchItemsSet.getLong("timestamp_ms") / 1000;
                    String text = searchItemsSet.getString("text");
                    String media_url = searchItemsSet.getString("media_url");
                    String media_playable_url = searchItemsSet.getString("media_playable_url");

                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_ID, moduleName, id));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID, moduleName, author_id));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT, moduleName, text != null ? text : ""));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL, moduleName, media_url != null ? media_url : ""));
                    attributes.add(new BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, timestamp));

                    BlackboardAttribute.Type attributeType = currentCase.getSleuthkitCase().getAttributeType("MEDIA_PLAYABLE_URL");
                    if (attributeType == null) {
                        attributeType = currentCase.getSleuthkitCase().addArtifactAttributeType("MEDIA_PLAYABLE_URL",
                                BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "media playable urle");
                    }

                    attributes.add(new BlackboardAttribute(attributeType, moduleName, media_playable_url));

                    BlackboardArtifact.Type artifactType = Case.getCurrentCase().getSleuthkitCase().getBlackboard().getOrAddArtifactType(
                            "FB_STORY", "Facebook Story", BlackboardArtifact.Category.DATA_ARTIFACT);

                    BlackboardArtifact artifact = content.newDataArtifact(artifactType, attributes);
                    artifacts.add(artifact);

                }

                blackboard.postArtifacts(artifacts, moduleName);
            }

        } catch (SQLException exception) {
            logger.log(Level.SEVERE, exception.getMessage(), exception);
        } catch (TskDataException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        } catch (TskCoreException exception) {
            logger.log(Level.SEVERE, "Failed to add FB Messenger call log artifacts.", exception);
        } catch (Blackboard.BlackboardException exception) {
            logger.log(Level.SEVERE, "Failed to post artifacts.", exception);
        }

        logger.log(Level.INFO, "Method END: analyzeStories");
    }

    private void extractOwnerFacebookId(AppSQLiteDB msysDb) {
        logger.log(Level.INFO, "Method start: extractOwnerFacebookId");
        String query = "select id, facebook_user_id from _user_info";
        try {
            ResultSet resultSet = msysDb.runQuery(query);

            if (resultSet != null) {

                while (resultSet.next()) {
                    ownerUserId = resultSet.getString("facebook_user_id");
                }
            }
        } catch (SQLException exception) {
            logger.log(Level.WARNING, exception.getMessage(), exception);
        }

        logger.log(Level.INFO, "Method end: extractOwnerFacebookId");
    }
    
    private void analyzeCache(){
        
    }
}
