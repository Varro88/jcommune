/**
 * Copyright (C) 2011  JTalks.org Team
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.jtalks.jcommune.model.discourse;

import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.UserContact;

import java.time.LocalDateTime;
import java.util.Iterator;

/**
 * User data in Discourse.
 *
 * @author Artem Reznyk
 */
public class DiscourseUser {

    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    //users
    private String username;
    private String email;
    private LocalDateTime updatedAt;
    private Boolean active;
    private LocalDateTime lastSeenAt;
    private Boolean admin = false;
    private LocalDateTime previousVisitAt;
    private Boolean blocked;
    private LocalDateTime firstSeenAt;
    private String name;

    //user_profiles
    private String location;
    private String website;

    //user_options
    private Boolean emailPrivateMessages;

    //users
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public LocalDateTime getLastSeenAt() {
        return lastSeenAt;
    }

    public void setLastSeenAt(LocalDateTime lastSeenAt) {
        this.lastSeenAt = lastSeenAt;
    }

    public Boolean getAdmin() {
        return admin;
    }

    public void setAdmin(Boolean admin) {
        this.admin = admin;
    }

    public LocalDateTime getPreviousVisitAt() {
        return previousVisitAt;
    }

    public void setPreviousVisitAt(LocalDateTime previousVisitAt) {
        this.previousVisitAt = previousVisitAt;
    }

    public Boolean getBlocked() {
        return blocked;
    }

    public void setBlocked(Boolean blocked) {
        this.blocked = blocked;
    }

    public LocalDateTime getFirstSeenAt() {
        return firstSeenAt;
    }

    public void setFirstSeenAt(LocalDateTime firstSeenAt) {
        this.firstSeenAt = firstSeenAt;
    }

    public String getName() {
        return  this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //user_profiles

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    //user_options

    public Boolean getEmailPrivateMessages() {
        return emailPrivateMessages;
    }

    public void setEmailPrivateMessages(Boolean emailPrivateMessages) {
        this.emailPrivateMessages = emailPrivateMessages;
    }

    private static final int bannedGroupId = 123456789;
    private static final int websiteContactId = 17;
    private static final String websiteContactName = "Website";

    public DiscourseUser(JCUser jcommuneUser) {
        this.id = (int)jcommuneUser.getId();
        this.username = jcommuneUser.getUsername();

        //this.email = jcommuneUser.getEmail();
        this.email = jcommuneUser.getId() + "@jcommune-mail.org";

        this.updatedAt = jodaToJavaLocalDateTime(jcommuneUser.getLastLogin());
        this.active = jcommuneUser.isEnabled();
        this.lastSeenAt = jodaToJavaLocalDateTime(jcommuneUser.getLastLogin());
        if (jcommuneUser.getRole().equals("ADMIN_ROLE")) {
            this.admin = true;
        }
        this.previousVisitAt = jodaToJavaLocalDateTime(jcommuneUser.getLastLogin());
        if (jcommuneUser.getGroupsIDs().contains(bannedGroupId)) {
            this.blocked = true;
        }
        this.firstSeenAt = jodaToJavaLocalDateTime(jcommuneUser.getRegistrationDate());

        this.location = jcommuneUser.getLocation();
        for(UserContact contact: jcommuneUser.getContacts()) {
            if(contact.getType().getId() == DiscourseMigration.WEBSITE_CONTACT_ID) {
                this.website = contact.getValue();
                break;
            }
        }

        this.emailPrivateMessages = jcommuneUser.isSendPmNotification();

        this.name = jcommuneUser.getFirstName() + " " + jcommuneUser.getLastName();
    }

    public static java.time.LocalDateTime jodaToJavaLocalDateTime( org.joda.time.DateTime dateTime ) {
        return java.time.LocalDateTime.of(
                dateTime.getYear(),
                dateTime.getMonthOfYear(),
                dateTime.getDayOfMonth(),
                dateTime.getHourOfDay(),
                dateTime.getMinuteOfHour(),
                dateTime.getSecondOfMinute());
    }
}
