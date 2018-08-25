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
package org.jtalks.jcommune.test

import org.jtalks.common.model.entity.Component
import org.jtalks.jcommune.model.dto.UserDto
import org.jtalks.jcommune.plugin.api.web.dto.json.JsonResponseStatus
import org.jtalks.jcommune.test.model.User
import org.jtalks.jcommune.test.service.ComponentService
import org.jtalks.jcommune.test.service.GroupsService
import org.jtalks.jcommune.test.utils.Groups
import org.jtalks.jcommune.test.utils.SpamRules
import org.jtalks.jcommune.test.utils.Users
import org.jtalks.jcommune.test.utils.exceptions.WrongResponseException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.security.access.AccessDeniedException
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.transaction.TransactionConfiguration
import org.springframework.test.context.web.WebAppConfiguration
import org.springframework.test.web.servlet.MvcResult
import org.springframework.transaction.annotation.Transactional
import spock.lang.Specification

import static junit.framework.Assert.assertEquals
import static org.jtalks.jcommune.service.security.AdministrationGroup.*
import static org.jtalks.jcommune.test.utils.Groups.randomDto
/**
 * @author Oleg Tkachenko
 */
@WebAppConfiguration
@ContextConfiguration(locations = 'classpath:/org/jtalks/jcommune/web/view/test-configuration.xml')
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
class AdministrationControllerTest extends Specification {
    @Autowired Users users
    @Autowired Users popUpUsers
    @Autowired Groups groups
    @Autowired GroupsService groupsService
    @Autowired ComponentService componentService
    @Autowired SpamRules spamRules
    private Component forum

    def setup() {
        groupsService.createPredefinedGroups()
        forum = componentService.createForumComponent()
    }

    def 'user without admin rights cannot see group administration page'() {
        given: 'user logged in but doesn`t have admin rights'
            def session = users.signInAsRegisteredUser(forum)
        when: 'user requests group administration page'
            groups.showGroupAdministrationPage(session)
        then: 'Access is denied'
            thrown(AccessDeniedException)
    }

    def 'must not be able to see group administration page for not authenticated users'() {
        when: 'anonymous user requests group administration page'
            groups.showGroupAdministrationPage(session = null)
        then: 'The session is missing, perform login'
            thrown(MissingPropertyException)
    }

    def 'user without admin rights cannot create groups'() {
        given: 'user logged in but doesn`t have admin rights'
            def session = users.signInAsRegisteredUser(forum)
        when: 'User attempts to create group'
            def groupDto = randomDto()
            groups.create(groupDto, session)
        then: 'Access is denied'
            thrown(AccessDeniedException)
    }

    def 'only user with admin rights can create group'() {
        given: 'User logged in and has admin rights'
            def session = users.signInAsAdmin(forum)
        when: 'User attempts to create group'
            def groupDto = randomDto()
            groups.create(groupDto,session)
        then: 'Group is created'
            groups.isExist(groupDto.name)
    }

    def 'user without admin rights cannot edit groups'() {
        given: 'user logged in but doesn`t have admin rights, random group created'
            def adminSession = users.signInAsAdmin()
            def savedGroupId = groups.create(randomDto(), adminSession)
        when: 'User attempts to edit an existing group'
            def userSession = users.signInAsRegisteredUser(forum)
            def groupDto = randomDto(id: savedGroupId)
            groups.edit(groupDto, userSession)
        then: 'Access is denied'
            thrown(AccessDeniedException)
    }

    def 'only user with admin rights can edit editable group'() {
        given: 'User logged in and has admin rights, random group created'
            def session = users.signInAsAdmin(forum)
            def savedGroupId = groups.create(randomDto(), session)
        when: 'User attempts to edit an existing editable group'
            def groupDto = randomDto(id: savedGroupId)
            groups.edit(groupDto, session)
        then: 'Group successfully edited'
            groups.isExist(groupDto.name)
            savedGroupId == groupsService.getIdByName(groupDto.name)
    }

    def 'must not be able to edit not existing group'() {
        given: 'User logged in and has admin rights, random group created'
            def session = users.signInAsAdmin(forum)
            def savedGroupId = groups.create(randomDto(), session)
        when: 'User attempts to edit not existing group'
            def groupDto = randomDto(id: savedGroupId + 1)
            groups.edit(groupDto, session)
        then: 'Group not found, error returned'
            thrown(WrongResponseException)
    }

    def 'must not be able to edit not editable group'() {
        given: 'User logged in and has admin rights'
            def session = users.signInAsAdmin(forum)
        when: 'User attempts to edit not editable group'
            def groupId = groupsService.getIdByName(notEditable.name)
            def groupDto = randomDto(id: groupId)
            groups.edit(groupDto, session)
        then: 'Validation error'
            thrown(WrongResponseException)
        and: 'Group is not edited'
            groups.assertDoesNotExist(groupDto.name)
        where: 'notEditable - list of not editable group names'
            notEditable << [ADMIN, USER, BANNED_USER]
    }

    def 'user with admin rights can delete group'() {
        given: 'User logged in and has admin rights'
            def session = users.signInAsAdmin(forum)
        and: 'group created'
            def groupDto = randomDto()
            groups.create(groupDto, session)
        when: 'User attempts to delete group'
            def groupId = groupsService.getIdByName(groupDto.name)
            groups.delete(groupId, session)
        then: 'group is deleted'
            groups.assertDoesNotExist(groupDto.name)
    }

    def 'must not be able to delete predefined group'() {
        given: 'User logged in and has admin rights'
            def session = users.signInAsAdmin(forum)
        when: 'User attempts to delete predefined group'
            def groupId = groupsService.getIdByName(notEditable.name)
            groups.delete(groupId, session)
        then: 'Validation error'
            thrown(WrongResponseException)
        and: 'Group is not deleted'
            groups.isExist(notEditable.name)
        where: 'notEditable - list of predefined group names'
            notEditable << [ADMIN, USER, BANNED_USER]
    }

    def 'must not be able to register with e-mail from blacklist through AJAX'(){
        given: 'spam rule created'
            def banned_email_domain = "mail.com"
            spamRules.createNewRule(".*@" + banned_email_domain)
        when: 'user tries to register with banned email address'
            def user = new User(email: 'some@' + banned_email_domain)
            popUpUsers.singUp(user)
        then:
            thrown(WrongResponseException)
        and: 'User is not created in database'
            popUpUsers.isNotExist(user.username)
    }

    def 'must not be able to register with e-mail from blacklist through HTML-form'(){
        given: 'spam rule created'
            def banned_email_domain = "mail.com"
            spamRules.createNewRule(".*@" + banned_email_domain)
        when: 'user tries to register with banned email address'
            def user = new User(email: 'some@' + banned_email_domain)
            users.singUp(user)
        then:
            thrown(WrongResponseException)
        and: 'User is not created in database'
            users.isNotExist(user.username)
    }

    def 'user in group should paginated with 20 per page'() {
        given: 'User logged in and has admin rights'
            def session = users.signInAsAdmin(forum)
        and: 'Group created'
            def groupDto = randomDto()
            groups.create(groupDto, session)
        and: 'Users in group created'
            users.createdCountInGroupWithoutAccess(45, groupDto.name)
            def group = groupsService.getGroupByName(groupDto.name)
            def groupUserList = group.users
            Collections.sort(groupUserList, new GroupsService.UserByNameComparator())
        when: 'User attempts to retrive paginated list of users in group'
            MvcResult page1 = groups.getPagedGroupUsers(group.id, 1L, session)
            MvcResult page2 = groups.getPagedGroupUsers(group.id, 2L, session)
            MvcResult page3 = groups.getPagedGroupUsers(group.id, 3L, session)
        then: 'List of users is paginated by 20 on page'
            groups.assertGroupUserPage(page1, groupUserList.subList(0, 20))
            groups.assertGroupUserPage(page2, groupUserList.subList(20, 40))
            groups.assertGroupUserPage(page3, groupUserList.subList(40, 45))
    }

    def 'interface of search user not in group should be appropriate'() {
        given: 'User logged in and has admin rights'
            def session = users.signInAsAdmin(forum)
        and: 'Two groups created'
            def groupDto1 = randomDto()
            def groupId1 = groups.create(groupDto1, session)
            def groupId2 = groupId1 + 1
        and: 'Users in first group'
            def user1group1 = users.createUserInGroup("f_user1gr1", "us1group1@mail.ru", groupDto1.name)
        when: 'Attempt to get filtered by name users out of first group'
            def jsonResponse = groups.getUserNotInGroupByPattern(groupId2, "user", session)
        then:
            assertEquals(JsonResponseStatus.SUCCESS, jsonResponse.status)
            groups.assertUserDtoList([user1group1], (List<UserDto>) jsonResponse.result)
    }
}
