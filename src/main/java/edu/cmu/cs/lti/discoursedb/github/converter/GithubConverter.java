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
import java.util.stream.Stream;

import java.util.zip.GZIPInputStream;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

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
import edu.cmu.cs.lti.discoursedb.github.model.GithubUserInfo;
import edu.cmu.cs.lti.discoursedb.github.model.MailingListComment;

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
    @PersistenceContext
    EntityManager entityManager;
	
	@Override
	public void run(String... args) throws Exception {

		//Parse command line param with dataset name
		final String dataSetName=args[0];		
		if(dataSourceService.dataSourceExists(dataSetName)){
			logger.warn("Dataset "+dataSetName+" has already been imported into DiscourseDB. Terminating...");			
			return;
		}
		
		
		
				
		//Parse command line param with dataset location. 
		// Conventions:
		//    issues/  contains issue conversations from github, and commits saved in issue0
		//    githubarchive/  contains raw hourly githubarchive files (in .json.gz format)
		//    mail_lists/  contains googlegroups or similar mailing list dumps
		//final Path dataSetPath = Paths.get(args[1] + "/issues");
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
				
		logger.info("Start processing users");		
			File usersFile = Paths.get(env.getRequiredProperty("gitdata.actors")).toFile();
			processUsersFile(usersFile);

		logger.info("Add clustering annotations to users");
			File userFactorsFile = Paths.get(env.getRequiredProperty("gitdata.user_factors")).toFile();
			processUserFactorsFile(userFactorsFile);
			
		logger.info("Add clustering annotations to projects");
			File projectFactorsFile = Paths.get(env.getRequiredProperty("gitdata.project_factors")).toFile();
			processProjectFactorsFile(projectFactorsFile);
			
			
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
		 
		logger.info("Read githubarchive hour files");
		try (Stream<Path> pathStream = Files.walk(Paths.get(env.getRequiredProperty("gitdata.githubarchive")))) {
			pathStream.filter(path -> path.toFile().isFile())
			.filter(path -> !path.endsWith(".json.gz"))
			.forEach(path -> processGithubarchiveHourFile(path.toFile()));
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
			while (it.hasNextValue()) {
				Map<String,String> factors = it.next();
				String name = factors.get("username");
				factors.remove("username");
				assert name != null : "Missing username in project_factors file";
				converterService.mapUserFactors(name, factors);
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
	private void processProjectFactorsFile(File file){
		
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {
			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<Map<String,String>> it = mapper.readerFor(Map.class).with(schema).readValues(in);
			while (it.hasNextValue()) {
				Map<String,String> factors = it.next();
				String name = factors.get("reponame");
				assert name != null : "Missing reponame in project_factors file";
				factors.remove("reponame");
				converterService.mapProjectFactors(name, factors);
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
	private void processGithubarchiveHourFile(File file){
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
									DiscoursePartInteractionTypes.FORK_FROM);
							break;   
						case "CommitCommentEvent":
							break;
						case "CreateEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.CREATE);
							break;   
						case "DeleteEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.DELETE);
							break;
						case "WatchEvent":
							converterService.mapUserRepoEvent(
									n.getActor(), n.getProjectFullName(), n.getCreatedAt(),
									DiscoursePartInteractionTypes.WATCH);
							break;

						}
					}  catch (Exception e) {
						logger.error(n.toString());
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
	 * Parses a dataset file, binds its contents to a POJO and passes it on to the DiscourseDB converter
	 * 
	 * @param file an dataset file to process
	 */
	private void processUsersFile(File file){
		logger.info("Processing "+file);

		try(InputStream in = new FileInputStream(file);) {

			CsvMapper mapper = new CsvMapper();
			CsvSchema schema = mapper.schemaWithHeader().withNullValue("None");
			MappingIterator<GithubUserInfo> it = mapper.readerFor(GithubUserInfo.class).with(schema).readValues(in);
			boolean first = true;
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