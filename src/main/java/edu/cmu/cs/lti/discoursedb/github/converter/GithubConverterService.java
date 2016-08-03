package edu.cmu.cs.lti.discoursedb.github.converter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import edu.cmu.cs.lti.discoursedb.core.model.TimedAnnotatableSourcedBE;
import edu.cmu.cs.lti.discoursedb.core.model.TypedTimedAnnotatableSourcedBE;
//import edu.cmu.cs.lti.discoursedb.core.model.annotation.AnnotationAggregate;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.AnnotationInstance;
import edu.cmu.cs.lti.discoursedb.core.model.annotation.Feature;
//import edu.cmu.cs.lti.discoursedb.core.model.annotation.FeatureType;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Content;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Contribution;
import edu.cmu.cs.lti.discoursedb.core.model.macro.Discourse;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscoursePart;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscoursePartContribution;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscoursePartRelation;
import edu.cmu.cs.lti.discoursedb.core.model.macro.DiscourseRelation;
import edu.cmu.cs.lti.discoursedb.core.model.system.DataSourceInstance;
import edu.cmu.cs.lti.discoursedb.core.model.user.ContributionInteraction;
import edu.cmu.cs.lti.discoursedb.core.model.user.DiscoursePartInteraction;
//import edu.cmu.cs.lti.discoursedb.core.model.user.DiscoursePartInteractionType;
import edu.cmu.cs.lti.discoursedb.core.model.user.User;
import edu.cmu.cs.lti.discoursedb.core.repository.macro.DiscoursePartRelationRepository;
import edu.cmu.cs.lti.discoursedb.core.service.annotation.AnnotationService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.ContentService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.ContributionService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.DiscoursePartService;
import edu.cmu.cs.lti.discoursedb.core.service.macro.DiscourseService;
import edu.cmu.cs.lti.discoursedb.core.service.system.DataSourceService;
import edu.cmu.cs.lti.discoursedb.core.service.user.UserPredicates;
import edu.cmu.cs.lti.discoursedb.core.service.user.UserService;
import edu.cmu.cs.lti.discoursedb.core.type.ContributionInteractionTypes;
import edu.cmu.cs.lti.discoursedb.core.type.ContributionTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DataSourceTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartInteractionTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartRelationTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartTypes;
import edu.cmu.cs.lti.discoursedb.core.type.DiscourseRelationTypes;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubCommitCommentEvent;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubCreateDeleteEvent;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubExternalSite;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubForkEvent;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubGollumEvent;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubIssueComment;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubPullReqCommits;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubPushEvent;
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
	}
	private DiscoursePart getDiscoursePartByDataSource(Discourse d, String entitySourceId, String entitySourceDescriptor, 
			DataSourceTypes sourceType, String datasetName, DiscoursePartTypes type) {
		return discoursePartService.createOrGetDiscoursePartByDataSource(d,entitySourceId, entitySourceDescriptor, 
				sourceType, datasetName, type);
	}
	private User getUser(Discourse d, String username) {
		return userService.createOrGetUser(d, username);
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
	
	public final String COMMIT_SHA = "owner/project#sha";
	
	/*
	 * Retrieve the SHA values of all commits in the database, so when we
	 * see references to them (pulls, pushes, commit comments) we can link
	 * directly to the contribution id without having to query the database again
	 */
	public Map<String,Long> getCommitShas() {
		HashMap<String,Long> shas = new HashMap<String,Long>();
		for (Contribution c: contributionService.findAllByType(ContributionTypes.GIT_COMMIT_MESSAGE)) {
			Optional<DataSourceInstance> ds = dataSourceService.findDataSource(c, COMMIT_SHA);
			if (ds.isPresent()) {
				shas.put(ds.get().getEntitySourceId(), c.getId());
			}
		}
		return shas;
	}

	
	public Set<String> getNondegenerateUsers() {
		Set<String> users = new HashSet<String>();
		for (User u: userService.findUsersWithoutAnnotation("Degenerate")) {
			users.add(u.getUsername());
		}
		return users;
	}
	public Set<String> getNondegenerateProjects() {
		Set<String> projects = new HashSet<String>();
		for (DiscoursePart dp:  discoursePartService.findDiscoursePartsWithoutAnnotation("Degenerate")) {
			projects.add(dp.getName());
		}
		return projects;
	}
	
	Set<String> alreadyDegenerateUser = new HashSet<String>();
	Set<String> alreadyDegenerateProject = new HashSet<String>();
	
	public User ensureUserExists(String actor, Set<String> users, Discourse curDiscourse) {
		User curUser = userService.createOrGetUser(curDiscourse, actor);
		if (!users.contains(actor) && !alreadyDegenerateUser.contains(actor)) {
			// Mark as degenerate
			AnnotationInstance dgen = annotationService.createTypedAnnotation("Degenerate");
			annotationService.addAnnotation(curUser, dgen);
			alreadyDegenerateUser.add(actor);
		}
		return curUser;
	}
	public DiscoursePart ensureProjectExists(String projectname, Set<String> projects, Discourse curDiscourse) {
		DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, projectname, DiscoursePartTypes.GITHUB_REPO);
		if (!projects.contains(projectname) && !alreadyDegenerateProject.contains(projectname)) {
			// mark as degenerate
			AnnotationInstance dgen = annotationService.createTypedAnnotation("Degenerate");
			annotationService.addAnnotation(projectDP, dgen);
			alreadyDegenerateProject.add(projectname);
		}
		return projectDP;
	}
	
	/**
	 * Records the time a user did something associated with a repository
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
		
		if (   users.contains(actor) ||  projects.contains(projectname) ) {
			User curUser = ensureUserExists(actor, users, curDiscourse);
			DiscoursePart projectDP = ensureProjectExists(projectname, projects, curDiscourse);
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(curUser, projectDP, eventtype);
			dpi.setStartTime(when);
		}
	}
	
	
	/**
	 * Records the time a user did something associated with a repository
	 *  
	 * @param cde   Event object
	 * @param users List of non-degenerate users
	 * @param projects List of non-degenerate projects
	 */
	public void mapUserCreateDeleteEvent(GitHubCreateDeleteEvent cde,
			Set<String> users, Set<String> projects) {

		// Only do this if EITHER the user OR the project are already in the database
		
		Discourse curDiscourse = getDiscourse("Github");
		
		if (   users.contains(cde.getActor()) ||  projects.contains(cde.getProject()) ) {
			User curUser = ensureUserExists(cde.getActor(), users, curDiscourse);
			DiscoursePart projectDP = ensureProjectExists(cde.getProject(), projects, curDiscourse);
			DiscoursePartInteractionTypes dpitype = cde.getEventType() == "CreateEvent"?
					DiscoursePartInteractionTypes.CREATE
					:DiscoursePartInteractionTypes.DELETE;
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(curUser, projectDP, dpitype);
			if (cde.getWhat() != null) {
				AnnotationInstance kind = annotationService.createTypedAnnotation("ArtifactAffected");
				annotationService.addFeature(kind, annotationService.createTypedFeature(cde.getWhat(), "ArtifactName"));
				annotationService.addFeature(kind, annotationService.createTypedFeature(cde.getWhatType(), "ArtifactType"));
			}
			dpi.setStartTime(cde.getCreatedAt());
		}
	}

	/**
	 * Records the time a user did something associated with a repository
	 *  
	 * @param fe   Event object
	 * @param users List of non-degenerate users
	 * @param projects List of non-degenerate projects
	 */
	public void mapUserForkEvent(GitHubForkEvent fe,
			Set<String> users, Set<String> projects) {

		// Only do this if EITHER the user OR the project are already in the database
		
		Discourse curDiscourse = getDiscourse("Github");
		
		if (   users.contains(fe.getActor()) ||  projects.contains(fe.getProject()) ) {
			User curUser = ensureUserExists(fe.getActor(), users, curDiscourse);
			DiscoursePart projectDP = ensureProjectExists(fe.getProject(), projects, curDiscourse);
			
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(curUser, projectDP, 
					DiscoursePartInteractionTypes.FORK_FROM);
			if (fe.getForkedTo() != null) {
				AnnotationInstance kind = annotationService.createTypedAnnotation("ForkedTo");
				annotationService.addFeature(kind, annotationService.createTypedFeature(fe.getForkedTo(), "ForkedToProject"));
			}
			dpi.setStartTime(fe.getCreatedAt());
		}
	}
	
	/**
	 * Records the time a user did something associated with a repository
	 *  
	 * @param fe   Event object
	 * @param users List of non-degenerate users
	 * @param projects List of non-degenerate projects
	 * 
	public void mapUserCommitMessageEvent(GitHubCommitCommentEvent cce,
			Set<String> users, Set<String> projects) {

		// Only do this if EITHER the user OR the project are already in the database
		
		Discourse curDiscourse = getDiscourse("Github");
		
		if (   users.contains(fe.getActor()) ||  projects.contains(fe.getProject()) ) {
			User curUser = ensureUserExists(fe.getActor(), users, curDiscourse);
			DiscoursePart projectDP = ensureProjectExists(fe.getProject(), projects, curDiscourse);
			
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(curUser, projectDP, 
					DiscoursePartInteractionTypes.FORK_FROM);
			AnnotationInstance kind = annotationService.createTypedAnnotation("ForkedTo");
			annotationService.addFeature(kind, annotationService.createTypedFeature(fe.getForkedTo(), "ForkedToProject"));
			dpi.setStartTime(fe.getCreatedAt());
		}
	}*/
	
	//From http://stackoverflow.com/questions/14981109/checking-utf-8-data-type-3-byte-or-4-byte-unicode
		public static boolean isEntirelyInBasicMultilingualPlane(String text) {
			if (text==null) { return true; }
		    for (int i = 0; i < text.length(); i++) {
		        if (Character.isSurrogate(text.charAt(i))) {
		            return false;
		        }
		    }
		    return true;
		}
		public static String sanitizeUtf8mb4(String text) {
			return text;
/*			if (isEntirelyInBasicMultilingualPlane(text)) {
				return text;
			} else {
				logger.info("Sanitizing " + text + " of utf8mb4 characters");
				return StringEscapeUtils.escapeJava(text);
			}*/
		}
	
	/**
	 * Records the time a user did something associated with a repository
	 *  
	 * @param fe   Event object
	 * @param users List of non-degenerate users
	 * @param projects List of non-degenerate projects
	 */
	public void mapCommitCommentEvent(GitHubCommitCommentEvent cce,
			Set<String> users, Set<String> projects, Long contribution_id) {

		// Only do this if the user AND NOT the project are already in the database
		// Because project commit comments are handled elsewhere
		
		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, cce.getProject(), DiscoursePartTypes.GITHUB_REPO);
		
		if (   users.contains(cce.getActor()) && !  projects.contains(cce.getProject()) ) {
			User curUser = ensureUserExists(cce.getActor(), users, curDiscourse);
			
			Content k = contentService.createContent();	
			k.setAuthor(curUser);
			k.setStartTime(cce.getCreatedAt());
			
			//if (cc.contains("👍")) {
			//	k.setText(StringEscapeUtils.escapeJava(cce.getCommitComment()));
			//} else {
			k.setText(sanitizeUtf8mb4(cce.getCommitComment()));
			Contribution co = contributionService.createTypedContribution(ContributionTypes.GITHUB_COMMIT_COMMENT);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(cce.getCreatedAt());
			discoursePartService.addContributionToDiscoursePart(co, projectDP);
			if (contribution_id != null) {
				Optional<Contribution> appliesTo = contributionService.findOne(contribution_id);
				if (appliesTo.isPresent()) {
					contributionService.createDiscourseRelation(co, appliesTo.get(), DiscourseRelationTypes.REPLY);
					for (DiscoursePartContribution dpc: appliesTo.get().getContributionPartOfDiscourseParts()) {
						extendDiscoursePartDates(dpc.getDiscoursePart(), cce.getCreatedAt());
						discoursePartService.addContributionToDiscoursePart(co, dpc.getDiscoursePart());
					}
				}
			}
			
			// CURRENTLY: NO DATA SOURCE
		}
	}

	
	/**
	 * Records the time a user watched a repository
	 *  
	 * @param ges GitHubExternalSite object
	 */
	public void mapExternalSite(GitHubExternalSite ges) {

		// Only do this if EITHER the user OR the project are already in the database
		
		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart projectDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, ges.getProject(), DiscoursePartTypes.GITHUB_REPO);
		AnnotationInstance extsite = annotationService.createTypedAnnotation("ExternalSite");
		annotationService.addAnnotation(projectDP, extsite);
		annotationService.addFeature(extsite,  annotationService.createTypedFeature(ges.getSiteType(), "external_site_type"));
		annotationService.addFeature(extsite,  annotationService.createTypedFeature(ges.getStyle(), "external_site_style"));
		annotationService.addFeature(extsite,  annotationService.createTypedFeature(ges.getCanonical(), "external_site_ident"));
		annotationService.addFeature(extsite,  annotationService.createTypedFeature(ges.getUrl(), "url"));
		
		dataSourceService.addSource(extsite, new DataSourceInstance(ges.getUrl(), "external_site_url", DataSourceTypes.GITHUB, "GITHUB"));
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
		DiscoursePart threadDP = getDiscoursePartByDataSource(curDiscourse, posting.getForumThreadIdentifier(), "ggroups:forum/threadid", DataSourceTypes.GITHUB, dataSourceName, DiscoursePartTypes.THREAD);
		if (posting.getResponseTo() == "") {
			discoursePartService.createDiscoursePartRelation(forumDP, threadDP, DiscoursePartRelationTypes.SUBPART);
		}
		if (threadDP.getName() == null) {
			threadDP.setName("Thread: " + posting.getTitle());
			threadDP.setStartTime(posting.getDate());
			threadDP.setType("THREAD");
		}
		threadDP.setEndTime(posting.getDate());
		
		User actor = getUser(curDiscourse, posting.getAuthorNameAndEmail());
		actor.setEmail(posting.getAuthorEmail());
		actor.setRealname(posting.getAuthorName());
		
		Content k = contentService.createContent();	
		k.setAuthor(actor);
		k.setStartTime(posting.getDate());
		if (posting.getTitle() != null && posting.getTitle().length() > 255) {
			logger.info("Title too long " + posting.getFullyQualifiedUniqueMessage() +  ": " + posting.getTitle() );
			k.setTitle(sanitizeUtf8mb4(posting.getTitle()).substring(0, 254));
		} else {
			k.setTitle(sanitizeUtf8mb4(posting.getTitle()));
		}
		k.setText(sanitizeUtf8mb4(posting.getBody()));
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
		dataSourceService.addSource(co,  new DataSourceInstance(StringUtils.left(posting.getFullyQualifiedUniqueMessage(),94), "ggroups#unique_message", DataSourceTypes.GITHUB, dataSourceName));
		
		//Add contribution to DiscoursePart
		discoursePartService.addContributionToDiscoursePart(co, threadDP);
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
			logger.error("Cannot match post with parent: " + posting.getFullyQualifiedUniqueMessage() + " parent " + posting.getFullyQualifiedResponseTo());
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
			dataSourceService.addSource(a, new DataSourceInstance(
					StringUtils.left("pypi_versions#" + packageFile,94), "versionfile", DataSourceTypes.GITHUB, "GITHUB" ));
	
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
	
	public void extendDiscoursePartDates(DiscoursePart dp, Date newdate) {
		if (dp.getStartTime() == null || dp.getStartTime().after(newdate)) {
			dp.setStartTime(newdate);
		}
		if (dp.getEndTime() == null || dp.getEndTime().before(newdate)) {
			dp.setEndTime(newdate);
		}
	}
	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public long mapIssueEntities(GitHubIssueComment p) {				
		Assert.notNull(p,"Cannot map relations for post. Post data was null.");

		if (p.getText() == null || p.getText() == "") {
			return 0L;
		}
		Discourse curDiscourse = getDiscourse("Github");
		
		DiscoursePart issueDP = getDiscoursePart(curDiscourse, p.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
		
		String actorname = p.getActor();
		if (actorname == null) { actorname = "unknown"; }
		switch (p.getRectype()) {
		case "pull_request_commit": 
		case "commit_messages": {
			User actor = getUser(curDiscourse, actorname);
			
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			
			if (p.getTitle() != null && p.getTitle().length() > 255) {
				logger.info("Title too long " + p.getTitle() );
				k.setTitle(sanitizeUtf8mb4(p.getTitle()).substring(0, 254));
			} else {
				k.setTitle(sanitizeUtf8mb4(p.getTitle()));
			}
			
			k.setText(sanitizeUtf8mb4(p.getText()));
			Contribution co = contributionService.createTypedContribution(ContributionTypes.GIT_COMMIT_MESSAGE);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			extendDiscoursePartDates(issueDP, p.getTime());
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
			dataSourceService.addSource(co, new DataSourceInstance(StringUtils.left(p.getProjectFullName() + "#" + p.getAction(), 94),  COMMIT_SHA, DataSourceTypes.GITHUB, "GITHUB"));
			return co.getId();
		}
		case "issue_title": {
			User actor = getUser(curDiscourse, actorname);
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			
			if (p.getTitle() != null && p.getTitle().length() > 255) {
				logger.info("Title too long " + p.getTitle() );
				k.setTitle(sanitizeUtf8mb4(p.getTitle()).substring(0, 254));
			} else {
				k.setTitle(sanitizeUtf8mb4(p.getTitle()));
			}
			
			k.setText(sanitizeUtf8mb4(p.getText()));
			Contribution co = contributionService.createTypedContribution(ContributionTypes.THREAD_STARTER);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			extendDiscoursePartDates(issueDP, p.getTime());
			dataSourceService.addSource(co, new DataSourceInstance(p.getIssueIdentifier(), "github#issue", DataSourceTypes.GITHUB, "GITHUB"));
			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
			return co.getId();
		}
		case "issue_closed": {
			User actor = getUser(curDiscourse, p.getActor());
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(actor, issueDP, DiscoursePartInteractionTypes.GITHUB_ISSUE_CLOSE);
			extendDiscoursePartDates(issueDP, p.getTime());
			dpi.setStartTime(p.getTime());
			return 0L;
		}
		case "pull_request_merged": {
			User actor = getUser(curDiscourse, p.getActor());
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(actor, issueDP, DiscoursePartInteractionTypes.GIT_PULL_REQUEST_MERGE);
			extendDiscoursePartDates(issueDP, p.getTime());
			dpi.setStartTime(p.getTime());
			return 0L;
		}
		case "issue_comment": {
			User actor = getUser(curDiscourse, p.getActor());
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			k.setText(sanitizeUtf8mb4(p.getText()));
			k.setTitle(p.getTitle());
			Contribution co = contributionService.createTypedContribution(ContributionTypes.POST);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			extendDiscoursePartDates(issueDP, p.getTime());
			/*Optional<Contribution> parent = contributionService.findOneByDataSource(p.getIssueIdentifier(), "github#issue", "GITHUB");
			if (!parent.isPresent()) {
				logger.error("cannot link to issue " + p.getIssueIdentifier());
			}
	  	    contributionService.createDiscourseRelation(parent.get(), co, DiscourseRelationTypes.DESCENDANT);
*/
			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
			return co.getId();
		}
		/*//pull_request_commit_comment, pull_request_history, commit_messages, readme, issue_event
		case "pull_request_commit_comment": {
			User actor = userService.createOrGetUser(curDiscourse, p.getActor());
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			k.setText(p.getText());
			Contribution co = contributionService.createTypedContribution(ContributionTypes.GITHUB_COMMIT_COMMENT);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			Optional<Contribution> parent = contributionService.findOneByDataSource(p.getIssueIdentifier(), "github#issue", "GITHUB");
			if (!parent.isPresent()) {
				logger.error("cannot link to issue " + p.getIssueIdentifier());
			}
			dataSourceService.addSource(co, new DataSourceInstance(p.getProjectFullName() + "#" + p.getProvenance(),  COMMIT_SHA, DataSourceTypes.GITHUB, "GITHUB"));
	  	    contributionService.createDiscourseRelation(parent.get(), co, DiscourseRelationTypes.DESCENDANT);

			//Add contribution to DiscoursePart
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
		}*/
		}
		

				
		logger.trace("Post mapping completed.");
		return 0L;
	}
	
	/**
	 * Maps a post to DiscourseDB entities.
	 * 
	 * @param p the post object to map to DiscourseDB
	 * @param dataSetName the name of the dataset the post was extracted from
	 */
	public void mapCommitCommentEntities(GitHubIssueComment p, Map<String,Long> commit_shas) {				
		Assert.notNull(p,"Cannot map relations for post. Post data was null.");

		Discourse curDiscourse = getDiscourse("Github");
		DiscoursePart issueDP = getDiscoursePart(curDiscourse, p.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
		String actorname = p.getActor();
		if (actorname == null) { actorname = "unknown"; }
		switch (p.getRectype()) {
		
		case "pull_request_commit_comment":
		case "commit_comments": {
			User actor = getUser(curDiscourse, actorname);
			
			Content k = contentService.createContent();	
			k.setAuthor(actor);
			k.setStartTime(p.getTime());
			
			// TO DO: extract from p.getTitle() -> (position, line, path)
			
			k.setText(sanitizeUtf8mb4(p.getText()));
			Contribution co = contributionService.createTypedContribution(ContributionTypes.GITHUB_COMMIT_COMMENT);
			co.setCurrentRevision(k);
			co.setFirstRevision(k);
			co.setStartTime(p.getTime());
			String comment_on_sha = p.getProjectFullName() + "#" + p.getAction();
			extendDiscoursePartDates(issueDP, p.getTime());
			if (commit_shas.containsKey(comment_on_sha)) {
				Optional<Contribution> appliesTo = contributionService.findOne(commit_shas.get(comment_on_sha));
				if (appliesTo.isPresent() == false) {
					logger.warn("Could not find pull request reference to project " + comment_on_sha);
				} else {
					appliesTo.get().getContributionPartOfDiscourseParts().forEach(
							dp -> discoursePartService.addContributionToDiscoursePart(co,dp.getDiscoursePart()));
					

					contributionService.createDiscourseRelation(co, appliesTo.get(), DiscourseRelationTypes.REPLY);
				}
			}
			discoursePartService.addContributionToDiscoursePart(co, issueDP);
			//dataSourceService.addSource(co, new DataSourceInstance(p.getProjectFullName() + "#" + p.getAction(), COMMIT_SHA, DataSourceTypes.GITHUB, "GITHUB"));
		}
		
		}
		

				
		logger.trace("Post mapping completed.");
	}
	
	
	public void mapPushEvent(GitHubPushEvent pe, Set<String> users, Set<String> projects, Map<String, Long> commit_shas,
			String[] shas) {
		
		if (   users.contains(pe.getActor()) ||  projects.contains(pe.getProject()) ) {

			List<Contribution> commits = new ArrayList<Contribution>();
			for (String sha : shas) {
				String source = pe.getProject() + "#" + sha;
				if (commit_shas.containsKey(source)) {
					Optional<Contribution> appliesTo = contributionService.findOne(commit_shas.get(source));
					if (appliesTo.isPresent()) {
						commits.add(appliesTo.get());
					}
				}
				
			}
			
			String pushname = "Push by " + pe.getActor() + " at " + pe.getCreatedAt().toString();		
			if (commits.size() > 0) {
				Discourse curDiscourse = getDiscourse("Github");

				User curUser = ensureUserExists(pe.getActor(), users, curDiscourse);
				DiscoursePart curProject = ensureProjectExists(pe.getProject(), projects, curDiscourse);
				DiscoursePart curPush = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse,
						pushname, DiscoursePartTypes.GIT_PUSH);

				logger.info("Found " + commits.size() + " commits for " + pushname);
				for (Contribution c : commits) {
					discoursePartService.addContributionToDiscoursePart(c, curPush);
					extendDiscoursePartDates(curPush, c.getStartTime());
				}
				discoursePartService.createDiscoursePartRelation(curProject, curPush, DiscoursePartRelationTypes.SUBPART);
				userService.createDiscoursePartInteraction(curUser, curProject, DiscoursePartInteractionTypes.GIT_PUSH);
			} else {
				logger.info("Found NO commits for " + pushname);
			}
			// CURRENTLY: NO DATA SOURCE
		}		
	}
	
	public void mapPushEventOld(GitHubPushEvent pe, Set<String> users, Set<String> projects, Map<String, Long> commit_shas,
			String[] shas) {
		Discourse curDiscourse = getDiscourse("Github");
		
		if (   users.contains(pe.getActor()) ||  projects.contains(pe.getProject()) ) {
			User curUser = ensureUserExists(pe.getActor(), users, curDiscourse);
			DiscoursePart curProject = ensureProjectExists(pe.getProject(), projects, curDiscourse);
			DiscoursePart curPush = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse,
					"Push by " + pe.getActor() + " at " + pe.getCreatedAt().toString(),
					DiscoursePartTypes.GIT_PUSH);
			curPush.setStartTime(pe.getCreatedAt());
			
			discoursePartService.createDiscoursePartRelation(curProject, curPush, DiscoursePartRelationTypes.SUBPART);
			discoursePartService.save(curPush);
			DiscoursePartInteraction dpi = userService.createDiscoursePartInteraction(curUser, curProject, DiscoursePartInteractionTypes.GIT_PUSH);
			for (String sha : shas) {
				String source = pe.getProject() + "#" + sha;
				if (commit_shas.containsKey(source)) {
					Optional<Contribution> appliesTo = contributionService.findOne(commit_shas.get(source));
					if (appliesTo.isPresent()) {
						discoursePartService.addContributionToDiscoursePart(appliesTo.get(), curPush);
					}
				}
				
			}
			// CURRENTLY: NO DATA SOURCE
		}		
	}
	
	
	public void mapPullRequestCommits(GitHubPullReqCommits prc, Set<String> users, Set<String> projects, Map<String,Long> commit_shas) {
		Discourse curDiscourse = getDiscourse("Github");


		if (commit_shas.containsKey(prc.getFullName() + "#" + prc.getSha())) {
			Optional<Contribution> appliesTo = contributionService.findOne(commit_shas.get(prc.getFullName() + "#" + prc.getSha()));
			if (appliesTo.isPresent() == false) {
				logger.warn("Could not find pull request reference to project " + prc.getFullName() + " sha " + prc.getSha());
			} else {
				
				//User committer = ensureUserExists(prc.getCommitter(), users, curDiscourse);
				//User author = ensureUserExists(prc.getAuthor(), users, curDiscourse);
				//DiscoursePart curProject = ensureProjectExists(prc.getFullName(), projects, curDiscourse);
				DiscoursePart issueDP = getDiscoursePart(curDiscourse, prc.getIssueIdentifier(), DiscoursePartTypes.GITHUB_ISSUE);
				DiscoursePartContribution dpc = discoursePartService.addContributionToDiscoursePart(appliesTo.get(), issueDP);
				dpc.setStartTime(prc.getCreatedAt());
				extendDiscoursePartDates(issueDP, prc.getCreatedAt());
			}
		} else {
			//logger.warn("Could not find pull request reference to project; no match for " + prc.getFullName() + " sha " + prc.getSha());			
		}
		// IGNORING AUTHOR AND COMMITTER FOR NOW
		// NO DATA SOURCE		
	}
	
	/*
	 * Represent a unique wiki page or the like that can have
	 * updates over time.
	 */
	public Map<String,Long> context_map = new HashMap<String,Long>();
	
	public void mapGollumEvent(GitHubGollumEvent ge, Set<String> users, Set<String> projects) {
		Discourse curDiscourse = getDiscourse("Github");

		if (   users.contains(ge.getActor()) ||  projects.contains(ge.getProject()) ) {
			User curUser = ensureUserExists(ge.getActor(), users, curDiscourse);
			DiscoursePart projectDP = ensureProjectExists(ge.getProject(), projects, curDiscourse);
			DiscoursePart wikiDP = discoursePartService.createOrGetTypedDiscoursePart(curDiscourse, ge.getProject() + "/wiki", 
					DiscoursePartTypes.GITHUB_WIKI);
			Contribution con = null;
			Content c = contentService.createContent();
			c.setStartTime(ge.getCreatedAt());
			c.setTitle(ge.getTitle());
			c.setText("(not captured)");
			this.extendDiscoursePartDates(wikiDP, ge.getCreatedAt());
			if (!context_map.containsKey(ge.getHtmlUrl())) {
				con = contributionService.createTypedContribution(ContributionTypes.WIKI_PAGE);
				con.setStartTime(ge.getCreatedAt());
				con.setFirstRevision(c);  // ASSUMPTION: they come in chronologically
				AnnotationInstance ai = annotationService.createTypedAnnotation("URL_WITH_REVISIONS");
				if (ge.getHtmlUrl() != null && ge.getHtmlUrl() != "") {
					annotationService.addFeature(ai, annotationService.createTypedFeature(ge.getHtmlUrl(), "LOCAL_URL"));
					dataSourceService.addSource(con, new DataSourceInstance(ge.getHtmlUrl() + "#" + ge.getSha(), 
							"local_url#sha", DataSourceTypes.GITHUB, "GITHUB"));
				}
			} else {
				con = contributionService.findOne(context_map.get(ge.getHtmlUrl())).get();
				con.getCurrentRevision().setNextRevision(c);
				con.getCurrentRevision().setEndTime(ge.getCreatedAt());
				con.setCurrentRevision(c);
			}

			discoursePartService.createDiscoursePartRelation(projectDP, wikiDP, DiscoursePartRelationTypes.SUBPART);
			discoursePartService.addContributionToDiscoursePart(con, wikiDP);
			ContributionInteraction ci = userService.createContributionInteraction(curUser, con, ContributionInteractionTypes.EDIT);
			ci.setStartTime(ge.getCreatedAt());
			ci.setContent(c);
			c.setAuthor(curUser);
			c.setTitle(sanitizeUtf8mb4(ge.getTitle()));
			c.setText(ge.getHtmlUrl());
		}
			
		// IGNORING AUTHOR AND COMMITTER FOR NOW
		// NO DATA SOURCE		
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