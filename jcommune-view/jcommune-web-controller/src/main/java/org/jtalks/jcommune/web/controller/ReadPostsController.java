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
package org.jtalks.jcommune.web.controller;

import org.jtalks.jcommune.model.entity.Branch;
import org.jtalks.jcommune.model.entity.Topic;
import org.jtalks.jcommune.service.BranchService;
import org.jtalks.jcommune.service.LastReadPostService;
import org.jtalks.jcommune.plugin.api.exceptions.NotFoundException;
import org.jtalks.jcommune.service.TopicFetchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handles all "mark all read" requests that aren't related to the list
 * of topics or to concrete branch. So it's handles specific "mark all read"
 * requests, for example "mark all forum as read from recent activity" and
 * "mark all forum as read from main page". 
 * 
 * @author Anuar_Nurmakanov
 *
 */
@Controller
public class ReadPostsController {
    private final BranchService branchService;
    private final TopicFetchService topicService;
    private final LastReadPostService lastReadPostService;
    private final RetryTemplate retryTemplate;

    /**
     * Constructs an instance with required fields.
     * 
     * @param to find branches which will be marked as read
     * @param lastReadPostService to mark all forum as read for current user
     */
    @Autowired
    public ReadPostsController(BranchService branchService, LastReadPostService lastReadPostService,
                               RetryTemplate retryTemplate, TopicFetchService topicService) {
        this.branchService = branchService;
        this.lastReadPostService = lastReadPostService;
        this.retryTemplate = retryTemplate;
        this.topicService = topicService;
    }

    /**
     * Mark all forum as read for current user from "recent activity" page.
     * Note, that we use GET method because it more suitable for working 
     * with <a href> element.
     */
    @RequestMapping(value = "/recent/forum/markread", method = RequestMethod.GET)
    public String markAllForumAsReadFromRecentActivity() {
        retryTemplate.execute(new RetryCallback<Void, RuntimeException>() {
            @Override
            public Void doWithRetry(RetryContext context) throws HibernateOptimisticLockingFailureException {
                lastReadPostService.markAllForumAsReadForCurrentUser();
                return null;
            }
        });
        return "redirect:/topics/recent";
    }
    
    /**
     * Mark all forum as read for current user from "main page" page.
     * Note, that we use GET method because it more suitable for working 
     * with <a href> element.
     */
    @RequestMapping(value = "/main/forum/markread", method = RequestMethod.GET)
    public String markAllForumAsReadFromMainPage() {
        lastReadPostService.markAllForumAsReadForCurrentUser();
        return "redirect:/sections";
    }
    
    /**
     * Marks all topics in branch as read regardless
     * of pagination settings or whatever else.
     *
     * @param id branch id to find the appropriate topics
     * @return redirect to the same branch page
     * @throws NotFoundException if no branch matches id given
     */
    @RequestMapping("/branches/{id}/markread")
        public String markAllTopicsAsRead(@PathVariable long id) throws NotFoundException {
        Branch branch = branchService.get(id);
        lastReadPostService.markAllTopicsAsRead(branch);
        return "redirect:/branches/" + id;
    }

    /**
     * Marks the specified page of the topic as read.
     *
     * @param topicId topic id to mark
     * @param pageNum page number to mark
     * @throws NotFoundException if the topic not found
     */
    @ResponseBody
    @RequestMapping("/topics/{topicId}/page/{pageNum}/markread")
    public String markTopicPageAsReadById(@PathVariable long topicId,
                                          @PathVariable int pageNum) throws NotFoundException {
        Topic topic = topicService.get(topicId);
        lastReadPostService.markTopicPageAsRead(topic, pageNum);
        return "";
    }
}
