package edu.cmu.cs.lti.discoursedb.github.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simple PoJo to represent rows in GitHub issue extracts in CSV format.<br/>
 * Expects files with the following header:<br/>
 * <code>rectype,issueid,project_owner,project_name,actor,time,text,action,title,provenance,plus_1,urls,issues,userref,code</code><br/>
 * 
 * @author Oliver Ferschke
 *
 */
public class GitHubWatchEvent {

	private static final Logger logger = LogManager.getLogger(GitHubWatchEvent.class);	
	
	private String type;
	private String actorName;
	private String actorEmail;
	private String repoName;
	private String action;
	private int contributionCount;
	private ArrayList<String> contTitles;
	private ArrayList<String> contBodies;
	private ArrayList<String> contReferences;
	private Date createdAt;
	
	public GitHubWatchEvent(){}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getActorEmail() {
		return actorEmail;
	}

	@JsonProperty("actor_email")
	public void setActorEmail(String actorEmail) {
		this.actorEmail = actorEmail;
	}

	public String getActorName() {
		return actorName;
	}

	@JsonProperty("actor_name")
	public void setActorName(String actorName) {
		this.actorName = actorName;
	}

	public int getContributionCount() {
		return contributionCount;
	}

	@JsonProperty("contribution_count")
	public void setContributionCount(int contributionCount) {
		this.contributionCount = contributionCount;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getRepoName() {
		return repoName;
	}

	@JsonProperty("repo_name")
	public void setRepoName(String repoName) {
		this.repoName = repoName;
	}

	public ArrayList<String> getContTitles() {
		return contTitles;
	}

	@JsonProperty("cont_titles")
	public void setContTitles(ArrayList<String> contTitles) {
		this.contTitles = contTitles;
	}

	public ArrayList<String> getContBodies() {
		return contBodies;
	}

	@JsonProperty("cont_bodies")
	public void setContBodies(ArrayList<String> contBodies) {
		this.contBodies = contBodies;
	}

	public ArrayList<String> getContReferences() {
		return contReferences;
	}

	@JsonProperty("cont_references")
	public void setContReferences(ArrayList<String> contReferences) {
		this.contReferences = contReferences;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	@JsonProperty("created_at")
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	
}
