package edu.cmu.cs.lti.discoursedb.github.converter;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import edu.cmu.cs.lti.discoursedb.core.model.annotation.AnnotationInstance;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.Feature;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.FeatureType;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Content;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Contribution;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Discourse;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscoursePart;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscourseRelation;
import edu.cmu.cs.lti.discoursedb.core.model.system.DataSourceInstance;
import edu.cmu.cs.lti.discoursedb.core.model.user.DiscoursePartInteraction;
import edu.cmu.cs.lti.discoursedb.core.model.user.DiscoursePartInteractionType;
import edu.cmu.cs.lti.discoursedb.core.model.user.User;
import edu.cmu.cs.lti.discoursedb.core.service.annotation.AnnotationService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.ContentService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.ContributionService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.DiscoursePartService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.DiscourseService;
import edu.cmu.cs.lti.discoursedb.core.service.system.DataSourceService;
import edu.cmu.cs.lti.discoursedb.core.service.user.UserPredicates;
import edu.cmu.cs.lti.discoursedb.core.service.user.UserService;
import edu.cmu.cs.lti.discoursedb.core.type.ContributionTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DataSourceTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartInteractionTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartRelationTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscourseRelationTypes;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubIssueComment;
import edu.cmu.cs.lti.discoursedb.github.model.GithubUserInfo;
import edu.cmu.cs.lti.discoursedb.github.model.MailingListComment;

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
	@Autowired private DataSourceService dataSourceService;
	@Autowired private AnnotationService annotationService;
	
	/**
	 * Maps a github Issue to DiscourseDB entities
	 *  
	 * @param p A github issue comment object
	 * 
	 */
	public void mapIssue(GitHubIssueComment p) { 
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart ownerDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, p.getProjectOwner(), DiscoursePartTypes.GITHUB_OWNER_REPOS);
		DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, p.getProjectFullName(), DiscoursePartTypes.GITHUB_REPO);
		DiscoursePart issueDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, p.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
		discoursePartService.createDiscoursePartRelation(ownerDP, projectDP, DiscoursePartRelationTypes.SUBPART);
		discoursePartService.createDiscoursePartRelation(projectDP, issueDP, DiscoursePartRelationTypes.SUBPART);
	}
	
	/**
	 * Records the time a user watched a repository
	 *  
	 * @param actor
	 * @param projectname (owner/repo)
	 * @param when (date)
	 * @param eventtype (kind of interaction)
	 */
	public void mapUserRepoEvent(String actor, String projectname, Date when, DiscoursePartInteractionTypes eventtype) {

		// Only do this if EITHER the user OR the project are already in the database
		
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		
		List<User> ifuser = userService.findUserByUsername(actor);
		List<DiscoursePart> ifdp = discoursePartService.findAllByName(projectname);
		
		if (ifuser.size() > 0 || ifdp.size() > 0) {
			User curUser = userService.createOrGetUser(curDiscourse, actor);
			if (ifuser.size() == 0) {
				// Mark as degenerate
				AnnotationInstance dgen = annotationService.createTypedAnnotation("Degenerate");
				annotationService.addAnnotation(curUser, dgen);
			}
			DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, projectname, DiscoursePartTypes.GITHUB_REPO);
			if (ifdp.size() == 0) {
				// mark as degenerate
				AnnotationInstance dgen = annotationService.createTypedAnnotation("Degenerate");
				annotationService.addAnnotation(projectDP, dgen);
			}
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(curUser, projectDP, eventtype);
			dpi.setStartTime(when);
			
		
		}
	}


	
	/**
	 * Maps a mailing list to DiscourseDB entities
	 *  
	 * @param owner       Github owner
	 * @param project     Github project (that this mailing list is sort of associated with)
	 * @param forumName   Google groups forum name
	 * @param internal    Does this mailing list really belong to the project?
	 */
	public void mapForum(String owner, String project, String fullForumName, boolean internal) {
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart forumDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, fullForumName, DiscoursePartTypes.FORUM);
		if (internal) {
			DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, owner + "/" + project, DiscoursePartTypes.GITHUB_REPO);
			discoursePartService.createDiscoursePartRelation(projectDP, forumDP, DiscoursePartRelationTypes.SUBPART);
		} else {
			// Do nothing for now.
			//
			// Note: if people in this project just kind of refer to a mailing list a lot, but it's not a part of the project,
			// then for now I'm creating no formal relation.
		}		
	}
	
	/**
	 * Maps a mailing list to DiscourseDB entities
	 *  
	 * @param owner       Github owner
	 * @param project     Github project (that this mailing list is sort of associated with)
	 * @param forumName   Google groups forum name
	 * @param internal    Does this mailing list really belong to the project?
	 */
	public void mapForumPost(MailingListComment posting, String dataSourceName) {
		// TODO: 2nd argument to findOneByDataSource should be a constant in an enum class
		
		// don't let it get added twice
		if (contributionService.findOneByDataSource(posting.getUniqueMessage(), "ggroups#unique_message", dataSourceName).isPresent()) {
			logger.error("Not re-adding post " + posting.getUniqueMessage());
			return;
		}
		
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart forumDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, posting.getFullForumName(), DiscoursePartTypes.FORUM);
		
		User actor = userService.createOrGetUser(curDiscourse, posting.getAuthorNameAndEmail());
		actor.setEmail(posting.getAuthorEmail());
		actor.setRealname(posting.getAuthorName());
		
		Content k = contentService.createContent();	
		k.setAuthor(actor);
		k.setStartTime(posting.getDate());
		k.setTitle(posting.getTitle());
		k.setText(posting.getBody());
		Contribution co = null;
		if (posting.getResponseTo() == "") {
			co = contributionService.createTypedContribution(ContributionTypes.THREAD_STARTER);
		} else {
			co = contributionService.createTypedContribution(ContributionTypes.POST);
		}
		co.setCurrentRevision(k);
		co.setFirstRevision(k);
		co.setStartTime(posting.getDate());
		dataSourceService.addSource(co,  new DataSourceInstance(posting.getUniqueMessage(), "ggroups#unique_message", DataSourceTypes.GITHUB, dataSourceName));
		
		//Add contribution to DiscoursePart
		discoursePartService.addContributionToDiscoursePart(co, forumDP);
	}
	
	/**
	 * Maps a mailing list to DiscourseDB entities
	 *  
	 * @param owner       Github owner
	 * @param project     Github project (that this mailing list is sort of associated with)
	 * @param forumName   Google groups forum name
	 * @param internal    Does this mailing list really belong to the project?
	 */
	public void mapForumPostRelation(MailingListComment posting, String dataSourceName) {
		// TODO: 2nd argument to findOneByDataSource should be a constant in an enum class
		
		if (posting.getResponseTo() == "") {
			return;
		}
		Optional<Contribution> thispost = contributionService.findOneByDataSource(posting.getUniqueMessage(), "ggroups#unique_message", dataSourceName);
		Optional<Contribution> parent = contributionService.findOneByDataSource(posting.getResponseTo(), "ggroups#unique_message", dataSourceName);
		if (!parent.isPresent()) {
			logger.error("Parent comment not found for forum " + posting.getOutsideForum() + " post " +  posting.getUniqueMessage());
			return;
		}
		
  	    contributionService.createDiscourseRelation(parent.get(), thispost.get(), DiscourseRelationTypes.DESCENDANT);
	}
	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapUserInfo(GithubUserInfo u) {	
		// TO DO: treat differently if it's deleted or if type=organization
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");

		try {
			User curUser = userService.createOrGetUser(curDiscourse, u.getLogin());
			if (!u.getType().equals("deleted")) {
				curUser.setLocation(u.getLocation());
				curUser.setEmail(u.getEmail());
				curUser.setRealname(u.getName());
				curUser.setStartTime(u.getCreatedAt());
				dataSourceService.addSource(curUser, new DataSourceInstance(u.getLogin(), "@github_user", DataSourceTypes.GITHUB, "GITHUB"));
			}		
		} catch (Exception e) {
			logger.trace("Error importing user info for " + u.getLogin() + ", " + e.getMessage());
		}
	}
	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapIssueEntities(GitHubIssueComment p) {				
		Assert.notNull(p,"Cannot map relations for post. Post data was null.");

		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart issueDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, p.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
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
			
			dataSourceService.addSource(co, new DataSourceInstance(p.getIssueIdentifier(), "github#issue", DataSourceTypes.GITHUB, "GITHUB"));
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
			Contribution co = contributionService.createTypedContribution(ContributionTypes.POST);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			Optional<Contribution> parent = contributionService.findOneByDataSource(p.getIssueIdentifier(), "github#issue", "GITHUB");
			if (!parent.isPresent()) {
				logger.error("cannot link to issue " + p.getIssueIdentifier());
			}
	  	    contributionService.createDiscourseRelation(parent.get(), co, DiscourseRelationTypes.DESCENDANT);

			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
		}
		}
		

				
		logger.trace("Post mapping completed.");
	}

	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 * 
	public void mapMailListEntities(MailingListComment p) {				
		Assert.notNull(p,"Cannot map relations for post. Post data was null.");
		
		Discourse curDiscourse = discourseService.createOrGetDiscourse("Github");
		DiscoursePart forumDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, p.getFullForumName(), DiscoursePartTypes.FORUM);
		String actorname = p.getAuthorName(); // THIS IS WRONG -- map to username first.
		
		

				
		logger.trace("Post mapping completed.");
	}*/

	

}