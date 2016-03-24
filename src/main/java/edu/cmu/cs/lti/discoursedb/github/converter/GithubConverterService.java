package edu.cmu.cs.lti.discoursedb.github.converter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import edu.cmu.cs.lti.discoursedb.core.model.TimedAnnotatableSourcedBE;
import edu.cmu.cs.lti.discoursedb.core.model.TypedTimedAnnotatableSourcedBE;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.AnnotationAggregate;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.AnnotationInstance;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.Feature;
//import edu.cmu.cs.lti.discoursedb.core.model.annotation.FeatureType;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Content;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Contribution;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Discourse;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscoursePart;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscourseRelation;
import edu.cmu.cs.lti.discoursedb.core.model.system.DataSourceInstance;
import edu.cmu.cs.lti.discoursedb.core.model.user.DiscoursePartInteraction;
//import edu.cmu.cs.lti.discoursedb.core.model.user.DiscoursePartInteractionType;
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
	
	private HashMap<String,Long> keyIndex = new HashMap<String,Long>();
	private HashMap<String,Long> dpKeyIndex = new HashMap<String,Long>();
	private HashMap<String,Long> userKeyIndex = new HashMap<String,Long>();
	private Discourse theDiscourse = null;
	private boolean globalTransaction = false;
	
	private Discourse getDiscourse(String name) {
		if (theDiscourse == null || !globalTransaction) {
			theDiscourse = discourseService.createOrGetDiscourse("Github");
		} 
		return theDiscourse;
	}
	private DiscoursePart getDiscoursePart(Discourse d, String name, DiscoursePartTypes typ) {
		return discoursePartService.createOrGetTypedDiscoursePart(d, name, typ);
		/*String lookupname = name + ":" + typ.toString();
		Long ix = dpKeyIndex.get(lookupname);
		if (ix == null) {
			DiscoursePart dp = discoursePartService.createOrGetTypedDiscoursePart(d, name, typ);
			if (dp != null) {
				dpKeyIndex.put(lookupname, dp.getId() );
			}
			return dp;
		} else {
			return discoursePartService.findOneById(ix);
			
		}*/
	}
	private User getUser(Discourse d, String username) {
		return userService.createOrGetUser(d, username);
		/*
		String lookupname =  username;
		Long ix = userKeyIndex.get(lookupname);
		if (ix == null) {
			User u = userService.createOrGetUser(d, username);
			if (u != null) {
				userKeyIndex.put(lookupname,  u.getId());
			}
			return u;
		} else {
			return userService.findOneById(ix);
		}*/
	}
	
	
	/**
	 * Maps a github Issue to DiscourseDB entities
	 *  
	 * @param p A github issue comment object
	 * 
	 */
	public void mapIssue(GitHubIssueComment p) { 
		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart ownerDP = getDiscoursePart(curDiscourse, p.getProjectOwner(), DiscoursePartTypes.GITHUB_OWNER_REPOS);
		DiscoursePart projectDP = getDiscoursePart(curDiscourse, p.getProjectFullName(), DiscoursePartTypes.GITHUB_REPO);
		DiscoursePart issueDP = getDiscoursePart(curDiscourse, p.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
		discoursePartService.createDiscoursePartRelation(ownerDP, projectDP, DiscoursePartRelationTypes.SUBPART);
		discoursePartService.createDiscoursePartRelation(projectDP, issueDP, DiscoursePartRelationTypes.SUBPART);
	}
	
	/*
	 * See if a database entity has a "Degenerate" annotation (meaning that we're not storing
	 * general information about this person or project; it's just a placeholder to indicate they had
	 * some interaction with an entity we do care about)
	 * 
	 * @param The User object to test
	 */
	public boolean isDegenerateU(TimedAnnotatableSourcedBE source) {
		if (source == null || source.getAnnotations() == null || source.getAnnotations().getAnnotations() == null ) {
			return false;
		}
		for (AnnotationInstance a : source.getAnnotations().getAnnotations()) {
			if (a.getType() == "Degenerate") {
				return true;
			}
		}
		return false;
	}
	
	/*
	 * See if a database entity has a "Degenerate" annotation (meaning that we're not storing
	 * general information about this person or project; it's just a placeholder to indicate they had
	 * some interaction with an entity we do care about)
	 * 
	 * @param The DiscoursePart object to test
	 */
	public boolean isDegenerateDp(TypedTimedAnnotatableSourcedBE source) {
		if (source == null || source.getAnnotations() == null || source.getAnnotations().getAnnotations() == null ) {
			return false;
		}
		for (AnnotationInstance a : source.getAnnotations().getAnnotations()) {
			if (a.getType() == "Degenerate") {
				return true;
			}
		}
		return false;
	}
	
	public Set<String> getNondegenerateUsers() {
		return userService.findUsersWithoutAnnotation("Degenerate");
	}
	public Set<String> getNondegenerateProjects() {
		return discoursePartService.findDiscoursePartsWithoutAnnotation("Degenerate");		
	}
	
	Set<String> alreadyDegenerateUser = new HashSet<String>();
	Set<String> alreadyDegenerateProject = new HashSet<String>();
	
	/**
	 * Records the time a user watched a repository
	 *  
	 * @param actor
	 * @param projectname (owner/repo)
	 * @param when (date)
	 * @param eventtype (kind of interaction)
	 */
	public void mapUserRepoEvent(String actor, String projectname, Date when, DiscoursePartInteractionTypes eventtype,
			Set<String> users, Set<String> projects) {

		// Only do this if EITHER the user OR the project are already in the database
		
		Discourse curDiscourse = getDiscourse("Github");
		
		
		//List<User> ifuser = userService.findUserByUsername(actor);
		//List<DiscoursePart> ifdp = discoursePartService.findAllByName(projectname);
		
		if (   users.contains(actor) ||  projects.contains(projectname) ) {
			User curUser = userService.createOrGetUser(curDiscourse, actor);
			if (!users.contains(actor) && !alreadyDegenerateUser.contains(actor)) {
				// Mark as degenerate
				AnnotationInstance dgen = annotationService.createTypedAnnotation("Degenerate");
				annotationService.addAnnotation(curUser, dgen);
				alreadyDegenerateUser.add(actor);
			}
			DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, projectname, DiscoursePartTypes.GITHUB_REPO);
			if (!projects.contains(projectname) && !alreadyDegenerateProject.contains(projectname)) {
				// mark as degenerate
				AnnotationInstance dgen = annotationService.createTypedAnnotation("Degenerate");
				annotationService.addAnnotation(projectDP, dgen);
				alreadyDegenerateProject.add(projectname);
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
		logger.info("Adding forum " + fullForumName);
		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart forumDP = getDiscoursePart(curDiscourse, fullForumName, DiscoursePartTypes.FORUM);
		if (internal) {
			DiscoursePart projectDP = getDiscoursePart(curDiscourse, owner + "/" + project, DiscoursePartTypes.GITHUB_REPO);
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
		// but scanning through all these is inefficient, so, maybe, actually, don't prevent this
		/*if (keyIndex.containsKey(posting.getFullyQualifiedUniqueMessage())) {
			logger.error("Not re-adding post " + posting.getFullyQualifiedUniqueMessage());
			return;			
		}
		if (contributionService.findOneByDataSource(posting.getFullyQualifiedUniqueMessage(), "ggroups#unique_message", dataSourceName).isPresent()) {
			logger.error("Not re-adding post " + posting.getFullyQualifiedUniqueMessage());
			return;
		}*/
		
		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart forumDP = getDiscoursePart(curDiscourse, posting.getFullForumName(), DiscoursePartTypes.FORUM);
		
		User actor = getUser(curDiscourse, posting.getAuthorNameAndEmail());
		actor.setEmail(posting.getAuthorEmail());
		actor.setRealname(posting.getAuthorName());
		
		Content k = contentService.createContent();	
		k.setAuthor(actor);
		k.setStartTime(posting.getDate());
		if (posting.getTitle() != null && posting.getTitle().length() > 255) {
			logger.info("Title too long " + posting.getFullyQualifiedUniqueMessage() +  ": " + posting.getTitle() );
			k.setTitle(posting.getTitle().substring(0, 254));
		} else {
			k.setTitle(posting.getTitle());
		}
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
		keyIndex.put(posting.getFullyQualifiedUniqueMessage(), co.getId());
		dataSourceService.addSource(co,  new DataSourceInstance(posting.getFullyQualifiedUniqueMessage(), "ggroups#unique_message", DataSourceTypes.GITHUB, dataSourceName));
		
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
		//Optional<Contribution> thispost = contributionService.findOneByDataSource(posting.getFullyQualifiedUniqueMessage(), "ggroups#unique_message", dataSourceName);
		//Optional<Contribution> parent = contributionService.findOneByDataSource(posting.getFullyQualifiedResponseTo(), "ggroups#unique_message", dataSourceName);
		Long postid = keyIndex.get(posting.getFullyQualifiedUniqueMessage());
		Long parentid = keyIndex.get(posting.getFullyQualifiedResponseTo());
		if (postid == null || parentid == null) {
			logger.error("ptrs are" + postid + ", " + parentid);
			logger.error("Cannot find post and parent of " + posting.getFullyQualifiedUniqueMessage() + " parent " + posting.getFullyQualifiedResponseTo());
			return;
		}
		try {
			Optional<Contribution> thispost = contributionService.findOne(postid); 
			Optional<Contribution> parent = contributionService.findOne(parentid);
			if (!parent.isPresent() || !thispost.isPresent()) {
				logger.error("Parent comment not found for " + posting.getFullyQualifiedUniqueMessage());
				return;
			}
		
			contributionService.createDiscourseRelation(parent.get(), thispost.get(), DiscourseRelationTypes.DESCENDANT);
		} catch (java.lang.IllegalArgumentException iae) {
			logger.error("Mapping forum post to parent: " + iae);
		}
	}
	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapUserInfo(GithubUserInfo u) {	
		// TO DO: treat differently if it's deleted or if type=organization
		Discourse curDiscourse = getDiscourse("Github");
		
		try {
			User curUser = getUser(curDiscourse, u.getLogin());
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
	 * Erases all annotations of type MATRIX_FACTORIZATION with a name feature matching the parameter
	 * 
	 * @param factorizationName   The name of the factorization
	 */
	public void deleteFactorization(String featureValue) {
		String annotationType = "MATRIX_FACTORIZATION";
		String featureType = "name";
		List<AnnotationInstance> as = annotationService.findAnnotationsByFeatureTypeAndValue(featureType, featureValue);
		for(AnnotationInstance a:as) {
			if (a.getType() == annotationType) {
				annotationService.deleteAnnotation(a);
			}
		}
	}	

	
	/**
	 * Maps a user's matrix factorization weights to features of an attribute.  The factorization is
	 * the output of an algorithm that clusters users and projects into a small set of factors, that is, 
	 * a small number of arbitrarily named features (e.g. F1, F2, etc).  Each user or project has
	 * a vector of floats corresponding to each of the features.
	 * 
	 * @param name: the user to attribute
	 * @param factorizationName: a user-friendly name for this factorization
	 * @param factorConfig: the name of some file that defines how this factorization was done
	 * @param factors: the factor weightings.
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapUserFactors(String name, String factorizationName, String factorConfig, Map<String, String> factors) {
		// TO DO: treat differently if it's deleted or if type=organization
		
		try {
			List<User> users = userService.findUserByUsername(name);
			if (users.size() == 0) { return; }
			User curUser = users.get(0);
			AnnotationInstance a = annotationService.createTypedAnnotation("MATRIX_FACTORIZATION");
			annotationService.addAnnotation(curUser, a);
			annotationService.addFeature(a,  annotationService.createTypedFeature(factorizationName, "name"));
			dataSourceService.addSource(a, new DataSourceInstance(factorConfig + "#" + name, "factorization_config_file", DataSourceTypes.GITHUB, "GITHUB"));
			
			for(String factorname: factors.keySet()) {
				Feature f = annotationService.createTypedFeature(factors.get(factorname), factorname);
				annotationService.addFeature(a, f);;
				
			}
		} catch (Exception e) {
			logger.info("Error classifying user info for " + name + ", " + e.getMessage());
		}
		
	}
	
	
	public void mapVersionInfo(String repo, String nameInRepo, String version, String packageFile, Date updated) {
		DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Discourse discourse = getDiscourse("Github");
			DiscoursePart dps = getDiscoursePart(discourse, repo, DiscoursePartTypes.GITHUB_REPO);
			AnnotationInstance a = annotationService.createTypedAnnotation("REVISION");
			annotationService.addAnnotation(dps, a);
			annotationService.addFeature(a, annotationService.createTypedFeature(version, "version"));
			annotationService.addFeature(a, annotationService.createTypedFeature(fmt.format(updated), "update_date"));
			annotationService.addFeature(a, annotationService.createTypedFeature(packageFile, "update_file"));
			dataSourceService.addSource(a, new DataSourceInstance("pypi_versions#" + packageFile, "versionfile", DataSourceTypes.GITHUB, "GITHUB" ));
	
		} catch (Exception e) {
			logger.trace("Error classifying project info for " + repo + ", " + e.getMessage());
		}
	}
	
	
	/**
	 * Maps a user's matrix factorization weights to features of an attribute
	 * 
	 * @param name: the user to attribute
	 * @param factors: the factor weightings.
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapProjectFactors(String name, String factorizationName, String factorConfig, Map<String, String> factors) {

		try {
			DiscoursePart dps = getDiscoursePart(getDiscourse("Github"), name, DiscoursePartTypes.GITHUB_REPO);
			AnnotationInstance a = annotationService.createTypedAnnotation("MATRIX_FACTORIZATION");
			annotationService.addAnnotation(dps, a);
			annotationService.addFeature(a,  annotationService.createTypedFeature(factorizationName, "name"));
			dataSourceService.addSource(a, new DataSourceInstance(factorConfig +"#" + name, "factorization_config_file", DataSourceTypes.GITHUB, "GITHUB"));
			for(String factorname: factors.keySet()) {
				annotationService.addFeature(a, annotationService.createTypedFeature(factors.get(factorname), factorname));
			}
			annotationService.addAnnotation(dps, a);
		} catch (Exception e) {
			logger.trace("Error classifying project info for " + name + ", " + e.getMessage());
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

		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart issueDP = getDiscoursePart(curDiscourse, p.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
		String actorname = p.getActor();
		if (actorname == null) { actorname = "unknown"; }
		switch (p.getRectype()) {
		case "issue_title": {
			User actor = getUser(curDiscourse, actorname);
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			
			if (p.getTitle() != null && p.getTitle().length() > 255) {
				logger.info("Title too long " + p.getTitle() );
				k.setTitle(p.getTitle().substring(0, 254));
			} else {
				k.setTitle(p.getTitle());
			}
			
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
			User actor = getUser(curDiscourse, p.getActor());
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			k.setText(p.getText());
			Contribution co = contributionService.createTypedContribution(ContributionTypes.POST);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			/*Optional<Contribution> parent = contributionService.findOneByDataSource(p.getIssueIdentifier(), "github#issue", "GITHUB");
			if (!parent.isPresent()) {
				logger.error("cannot link to issue " + p.getIssueIdentifier());
			}
	  	    contributionService.createDiscourseRelation(parent.get(), co, DiscourseRelationTypes.DESCENDANT);
*/
			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
		}
		break;  
		/*//pull_request_commit_comment, pull_request_history, commit_messages, readme, issue_event
		case "pull_request_commit_comment": {
			User actor = userService.createOrGetUser(curDiscourse, p.getActor());
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			k.setText(p.getText());
			Contribution co = contributionService.createTypedContribution(ContributionTypes.COMMIT_COMMENT);
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
		}*/
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