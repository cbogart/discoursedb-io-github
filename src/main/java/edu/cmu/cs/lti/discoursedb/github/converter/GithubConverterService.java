package edu.cmu.cs.lti.discoursedb.github.converter;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import edu.cmu.cs.lti.discoursedb.core.model.macro.Content;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Contribution;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Discourse;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscoursePart;
import edu.cmu.cs.lti.discoursedb.core.model.user.User;
import edu.cmu.cs.lti.discoursedb.core.service.macro.ContentService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.ContributionService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.DiscoursePartService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.DiscourseService;
import edu.cmu.cs.lti.discoursedb.core.service.user.UserService;
import edu.cmu.cs.lti.discoursedb.core.type.ContributionTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartRelationTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartTypes;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubIssueComment;

/**
 * 
 * This class is responsible to process data chunks provided by the GithubConverter and store them in DiscourseDB using DiscourseDB Service classes or (if necessary) repositories.
 * Each method in this class is run transactionally (each method inherits the class-level Transactional annotation)
 * 
 * @author Oliver Ferschke
 *
 */
@Transactional(propagation= Propagation.REQUIRED, readOnly=false)
@Service
public class GithubConverterService{

	
	private static final Logger logger = LogManager.getLogger(GithubConverterService.class);
	
	@Autowired private DiscourseService discourseService;
 	@Autowired private UserService userService;
	@Autowired private ContentService contentService;
	@Autowired private ContributionService contributionService;
	@Autowired private DiscoursePartService discoursePartService;
	
	/**
	 * Maps a github Issue to DiscourseDB entities
	 *  
	 * @param owner
	 * @param project
	 * @param IssueNum
	 */
	public void mapIssue(String owner, String project, long IssueNum) {
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart ownerDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, owner, DiscoursePartTypes.GITHUB_OWNER_REPOS);
		DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, project, DiscoursePartTypes.GITHUB_REPO);
		DiscoursePart issueDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, "Issue #" + IssueNum, DiscoursePartTypes.GITHUB_ISSUE);
		discoursePartService.createDiscoursePartRelation(ownerDP, projectDP, DiscoursePartRelationTypes.SUBPART);
		discoursePartService.createDiscoursePartRelation(projectDP, issueDP, DiscoursePartRelationTypes.SUBPART);
	}
	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapEntities(GitHubIssueComment p) {				
		Assert.notNull(p,"Cannot map relations for post. Post data was null.");
		
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart issueDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, "Issue #" + p.getIssueid(), DiscoursePartTypes.GITHUB_ISSUE);
		String actorname = p.getActor();
		if (actorname == null) { actorname = "unknown"; }
		switch (p.getRectype()) {
		case "issue_title": {
			User actor = userService.createOrGetUser(curDiscourse, actorname);
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			k.setTitle(p.getTitle());
			k.setText(p.getText());
			Contribution co = contributionService.createTypedContribution(ContributionTypes.THREAD_STARTER);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			
			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
		}
		break;
		case "issue_comment": {
			User actor = userService.createOrGetUser(curDiscourse, p.getActor());
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			k.setText(p.getText());
			Contribution co = contributionService.createTypedContribution(ContributionTypes.THREAD_STARTER);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			
			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
		}
		}
		

				
		logger.trace("Post mapping completed.");
	}


}