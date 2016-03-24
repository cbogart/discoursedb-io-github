package edu.cmu.cs.lti.discoursedb.github.converter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import java.util.zip.GZIPInputStream;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import edu.cmu.cs.lti.discoursedb.configuration.BaseConfiguration;
import edu.cmu.cs.lti.discoursedb.core.service.system.DataSourceService;
import edu.cmu.cs.lti.discoursedb.core.type.DiscoursePartInteractionTypes;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubArchiveEvent;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubIssueComment;
import edu.cmu.cs.lti.discoursedb.github.model.GitHubWatcherList;
import edu.cmu.cs.lti.discoursedb.github.model.GithubUserInfo;
import edu.cmu.cs.lti.discoursedb.github.model.MailingListComment;
import edu.cmu.cs.lti.discoursedb.github.model.RevisionEvent;

/**
 * This component will be discovered by the starter class <code>GithubConverterApplication</code>.<br/>
 * Since this class implements CommandLineRuner, the <code>run</code> method will receive the args of the main method of the starter class.<br/>
 * 
 * The Order annotations is not necessary. It allows to specify the order of execution in case we have multiple components.
 * This class can directly access any DiscourseDB repositories and services by Autowiring them, but it is recommended to wrap all interactions in methods located in teh <code>GithubConverterSetrvice</code> class, which is already autowired in this stub.
 * The main reason for this is that all calls to service methods will run in separate transactions.
 * 
 * @author Oliver Ferschke
 * @author Chris Bogart
 */
@Component
@Order(1)
@PropertySources({
	@PropertySource(value = "classpath:custom.properties", ignoreResourceNotFound = true) //optional custom config. keys specified here override defaults 
})
public class GithubConverter implements CommandLineRunner {

	private static final Logger logger = LogManager.getLogger(GithubConverter.class);	

	@Autowired private DataSourceService dataSourceService;
	@Autowired private GithubConverterService converterService;
	@Autowired private Environment env;
	
	@Override
	public void run(String... args) throws Exception {

		//Parse command line param with dataset name
		final String dataSetName=args[0];		
		if(dataSourceService.dataSourceExists(dataSetName)){
			logger.warn("Dataset "+dataSetName+" has already been imported into DiscourseDB. Terminating...");			
			return;
		}
		
		/*
		 * For each entry in the custom.properties file, import the relevant
		 * data iff the property exists, i.e. if a file name has been supplied.
		 * This gives fine-grained control over what is imported.
		 * 		
		 */
		if (env.containsProperty("gitdata.issues")) {
			final Path dataSetPath = Paths.get(env.getRequiredProperty("gitdata.issues"));
			File dataSetFile = dataSetPath.toFile();
			if (!dataSetFile.exists() || dataSetFile.isFile() || !dataSetFile.canRead()) {
				logger.error("Provided location (" + dataSetFile.getAbsolutePath() + ") is a file and not a directory.");
				throw new RuntimeException("Can't read directory "+dataSetPath);
			}
			//Walk through dataset directory and parse each file
	
			logger.info("Start processing issues");			
	
			try (Stream<Path> pathStream = Files.walk(dataSetPath)) {
				pathStream.filter(path -> path.toFile().isFile())
				.filter(path -> !path.endsWith(".csv"))
				.forEach(path -> processIssuesFile(path.toFile()));
			}				
		} else {
			logger.info("No gitdata.issues in custom.properties");
		}
		
		if (env.containsProperty("gitdata.actors")) {
			logger.info("Start processing actors");		
				File actorsFile = Paths.get(env.getRequiredProperty("gitdata.actors")).toFile();
				processActorsFile(actorsFile);
		} else {
			logger.info("No gitdata.actors in custom.properties");
		}
		
		if (env.containsProperty("gitdata.user_factors")) {
			logger.info("Add clustering annotations to users");
				File userFactorsFile = Paths.get(env.getRequiredProperty("gitdata.user_factors")).toFile();
				processUserFactorsFile(userFactorsFile);
		} else {
			logger.info("no gitdata.user_factors in custom.properties");
		}
			
		if (env.containsProperty("gitdata.project_factors")) {
			logger.info("Add clustering annotations to projects");
				File projectFactorsFile = Paths.get(env.getRequiredProperty("gitdata.project_factors")).toFile();
				processProjectFactorsFile(projectFactorsFile);
		} else {
			logger.info("No gitdata.project_factors in custom.properties");
		}
	
		if (env.containsProperty("gitdata.mail_lists")) {
			logger.info("Start processing fora");
				try (Stream<Path> pathStream = Files.walk(Paths.get(env.getRequiredProperty("gitdata.mail_lists")))) {
					pathStream.filter(path -> path.toFile().isFile())
						 .filter(path -> !path.endsWith(".csv"))
						 .forEach(path -> processForumFile(path.toFile()));
				}				
			logger.info("Reprocess fora to get thread links");
				try (Stream<Path> pathStream = Files.walk(Paths.get(env.getRequiredProperty("gitdata.mail_lists")))) {
					pathStream.filter(path -> path.toFile().isFile())
						 .filter(path -> !path.endsWith(".csv"))
						 .forEach(path -> reprocessForumFileForRelationships(path.toFile()));
				}				
		} else {
			logger.info("No gitdata.mail_lists in custom.properties");
		}
		

		if (env.containsProperty("gitdata.githubarchive")) {
			logger.info("Read githubarchive hour files");
			Set<String> users = converterService.getNondegenerateUsers();
			logger.info("   ...for " + users.size() + " users");
			Set<String> projects = converterService.getNondegenerateProjects();
			logger.info("   ...for " + projects.size() + " repositories");
			try (Stream<Path> pathStream = Files.walk(Paths.get(env.getRequiredProperty("gitdata.githubarchive")))) {
				pathStream.filter(path -> path.toFile().isFile())
				.filter(path -> !path.endsWith(".json.gz"))
				.forEach(path -> processGithubarchiveHourFile(path.toFile(), users, projects));
			}			
		} else {
			logger.info("No gitdata.githubarchive in custom.properties");
		}
		
		if (env.containsProperty("gitdata.watchers")) {
			logger.info("Add watch events to users and projects");
				Set<String> users = converterService.getNondegenerateUsers();
				logger.info("   ...for " + users.size() + " users");
				Set<String> projects = converterService.getNondegenerateProjects();
				logger.info("   ...for " + projects.size() + " repositories");
				File watchersFile = Paths.get(env.getRequiredProperty("gitdata.watchers")).toFile();
				processWatchersFile(watchersFile, users, projects);
		} else {
			logger.info("no gitdata.watchers in custom.properties");
		}

		
		if (env.containsProperty("gitdata.version_history")) {
			logger.info("Read version history file");
			File versionHistoryFile = Paths.get(env.getRequiredProperty("gitdata.version_history")).toFile();
			processVersionHistoryFile(versionHistoryFile);
		}
		logger.info("All done.");
	}

	/**
	 * Parses a dataset file, binds its contents to a POJO and passes it on to the DiscourseDB converter
	 * 
	 * @param file an dataset file to process
	 */
	private void processIssuesFile(File file){
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<GitHubIssueComment> it = mapper.readerFor(GitHubIssueComment.class).with(schema).readValues(in);
			boolean first = true;
			while (it.hasNextValue()) {
				GitHubIssueComment currentComment = it.next();
				if (first) {
					converterService.mapIssue(currentComment);
					first = false;
				}
				converterService.mapIssueEntities(currentComment);
				//TODO pass each data point to the converterService which then performs the mapping in a transaction
				//don't perform the mapping here directly, since it would not allow the transactions to be
				//work properly

			}
		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}

	/**
	 * Parses a dataset file, binds its contents to a POJO and passes it on to the DiscourseDB converter
	 * 
	 * @param file an dataset file to process
	 */
	private void processUserFactorsFile(File file){
		
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<Map<String,String>> it = mapper.readerFor(Map.class).with(schema).readValues(in);
			boolean first = true;
			while (it.hasNextValue()) {
				Map<String,String> factors = it.next();
				String name = factors.remove("username");
				assert name != null : "Missing username in user_factors file";
				
				String factorType = factors.remove("factorization_name");
				assert factorType != null : "Missing factorization_name in user_factors file";
				
				String factorConfig = factors.remove("config");
				assert factorConfig != null : "Missing config in user_factors file";
				if (first) {
					converterService.deleteFactorization(factorType);
					first = false;
				}

				converterService.mapUserFactors(name, factorType, factorConfig, factors);
			}
		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}

	/**
	 * Parses a pypi_versions.csv file and calls converterService to process it.  
	 * 
	 * Example header plus one row:
	 * 
	 * project_owner,project_name,pypi_name,pypi_rawname,version,upload_time,python_version,filename
	 * skwashd,python-acquia-cloud,acapi,acapi,0.4.1,2015-11-21 09:30:17,source,acapi-0.4.1.tar.gz
	 * 
	 * @param filename to process
	 */
	private void processVersionHistoryFile(File file) {
		logger.info("Processing " + file);
		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<RevisionEvent> it = mapper.readerFor(RevisionEvent.class).with(schema).readValues(in);
			boolean first = true;
			while (it.hasNextValue()) {
				RevisionEvent revision = it.next();
				converterService.mapVersionInfo(
						revision.getProjectFullName(),
						revision.getPypiName(),
						revision.getVersion(), revision.getFilename() + "?" + revision.getPythonVersion(),
						revision.getUploadTime()
						);
			}
		} catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}  
	}

	/**
	 * Parses a CSV file listing who watched what project when, 
	 * binds its contents to a GitHubWatcherList object,
	 * and passes it on to the DiscourseDB converter
	 *
	 * File format example:
	 * 
	 * actor,project,created_at
	 * F21,danielstjules/Stringy,2015-01-01T00:01:53Z
     * radlws,tomchristie/django-rest-framework,2015-01-01T00:05:29Z
     * 
	 * @param file a dataset file to process
	 */
	private void processWatchersFile(File file, Set<String> users, Set<String> projects){
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<GitHubWatcherList> it = mapper.readerFor(GitHubWatcherList.class).with(schema).readValues(in);
			while (it.hasNextValue()) {
				GitHubWatcherList gwl = it.next();
				converterService.mapUserRepoEvent(
						gwl.getActor(), gwl.getProject(), gwl.getCreatedAt(),
						DiscoursePartInteractionTypes.WATCH,
						users, projects);
			}
		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
		
	}

	/**
	 * Parses a project_factors.csv file, 
	 * binds its contents to a JsonNode and passes it on to the DiscourseDB converter
	 * 
	 * These represent a matrix factorization: the values of some arbitrary number of learned factors
	 * associated with each project.  The number of factors is the same throughout the file, but
	 * is not known at compile time.
	 * 
	 * Example lines from project_factors.csv file, in this example there are seven factors:
	 * 
	 * reponame,F1,F2,F3,F4,F5,F6,F7,factorization_name,config
	 * 5monkeys/django-enumfield,0.031645,5.6692e-06,0,0.00021528,0,0,0,LogMatrixFactorization,project_factors.csv
	 * 
	 * @param file a dataset file to process
	 */
	private void processProjectFactorsFile(File file){
		
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<Map<String,String>> it = mapper.readerFor(Map.class).with(schema).readValues(in);
			boolean first = true;
			while (it.hasNextValue()) {
				Map<String,String> factors = it.next();
				String name = factors.remove("reponame");
				assert name != null : "Missing reponame in project_factors file";
				
				String factorType = factors.remove("factorization_name");
				assert factorType != null : "Missing factorization_name in project_factors file";
				
				String factorConfig = factors.remove("config");
				assert factorConfig != null : "Missing config in project_factors file";				
				
				if (first) {
					converterService.deleteFactorization(factorType);
					first = false;
				}
				converterService.mapProjectFactors(name, factorType, factorConfig, factors);
			}
		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}

	/**
	 * Parses a dataset file, binds its contents to a JsonNode and passes it on to the DiscourseDB converter
	 * Only imports records where EITHER the user or the project are in a set of users/projects of interest.
	 * Unknown users associated with known projects, and unknown projects associated with known users, are
	 * added to the database as "degenerate" entries.
	 * 
	 * The input file is any standard hourly datafile downloaded from githubarchive.org
	 * 
	 * @param file       name of githubarchive file
	 * @param users      Set of usernames whose activity is of interest.  
	 * @param projects   Set of projects (string in owner/repo format) whose activity is of interest
	 */
	private void processGithubarchiveHourFile(File file, Set<String> users, Set<String> projects){
		logger.info("Processing "+file);


		try(InputStream in = new GZIPInputStream(new FileInputStream(file));) {
			final ObjectMapper mapper = new ObjectMapper();
			final MapType type = mapper.getTypeFactory().constructMapType(
					Map.class, String.class, Object.class);

			try {  //http://stackoverflow.com/questions/10411020/jackson-multiple-objects-and-huge-json-files
				for (Iterator it = new ObjectMapper().readValues(new JsonFactory().createParser(in), JsonNode.class); it.hasNext(); ) {
					GitHubArchiveEvent n = new GitHubArchiveEvent((JsonNode)it.next());
					try {

						switch (n.getRecordType()) {
						case "PushEvent":
						case "PullRequestEvent":
						case "IssuesEvent":
						case "IssueCommentEvent":
						case "DownloadEvent":
						case "GistEvent":
						case "GollumEvent":
							break;
						case "ForkEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.FORK_FROM,
									users, projects);
							break;   
						case "CommitCommentEvent":
							break;
						case "CreateEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.CREATE,
									users, projects);
							break;   
						case "DeleteEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.DELETE, 
									users, projects);
							break;
						case "WatchEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.WATCH,
									users, projects);
							break;

						}
					}  catch (Exception e) {
						logger.error(n.toString());
						e.printStackTrace();
						logger.error(e);
					}
				}

			}

			finally { in.close(); }



		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}

	/**
	 * Parses a user information file, binds its contents to a POJO and passes it on to the DiscourseDB converter
	 * 
	 * Sample rows from table:
	 * 
	 * location,login,name,email,company,email,blog,name,type,site_admin,bio,public_repos,public_gists,followers,following,created_at,updated_at,error
     * Colombia,acala,Ariel Calderon,,ACala,,http://www.acala.co,Ariel Calderon,User,False,,2,0,0,0,2012-11-09T23:09:14Z,2016-01-29T14:28:57Z,
     * "Rome, Italy",spork,Enrico Sporka,esporka@gmail.com,,esporka@gmail.com,http://spork.github.io,Enrico Sporka,User,False,,3,0,4,5,2015-07-25T10:02:08Z,2015-12-05T11:04:54Z,
	 * 
	 * @param file a file to process
	 */
	private void processActorsFile(File file){
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {

			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<GithubUserInfo> it = mapper.readerFor(GithubUserInfo.class).with(schema).readValues(in);
			while (it.hasNextValue()) {
				GithubUserInfo currentUser = it.next();

				converterService.mapUserInfo(currentUser);

			}
		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}

	/**
	 * Parses a dataset file, binds its contents to a POJO and passes it on to the DiscourseDB converter
	 * 
	 * @param file an dataset file to process
	 */
	private void processForumFile(File file){
		// project_owner,project_name,outside_forum_id,unique_message_id,date,author_email,author_name,
		//    title,body,response_to_message_id,thread_path,message_path

		logger.info("Processing "+file);
		
		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");

			MappingIterator<MailingListComment> it = mapper.readerFor(MailingListComment.class).with(schema).readValues(in);
			boolean first = true;
			while (it.hasNextValue()) {
				MailingListComment currentPost = it.next();
				if (first) {
					converterService.mapForum(currentPost.getProjectOwner(), currentPost.getProjectName(), 
							currentPost.getFullForumName(), true);
					first = false;
				}
				converterService.mapForumPost(currentPost, "GOOGLE_GROUPS");    
			}

		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}

	/**
	 * Parses a dataset file, binds its contents to a POJO and passes it on to the DiscourseDB converter
	 * 
	 * @param file an dataset file to process
	 */
	private void reprocessForumFileForRelationships(File file){
		// project_owner,project_name,outside_forum_id,unique_message_id,date,author_email,author_name,
		//    title,body,response_to_message_id,thread_path,message_path

		logger.info("Processing "+file);

		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
		try(InputStream in = new FileInputStream(file);) {

			MappingIterator<MailingListComment> it = mapper.readerFor(MailingListComment.class).with(schema).readValues(in);
			while (it.hasNextValue()) {
				MailingListComment currentPost = it.next();	
				converterService.mapForumPostRelation(currentPost, "GOOGLE_GROUPS");        			
			}

		}catch(Exception e){
			logger.error("Could not parse data file "+file, e);
		}    		
	}


}