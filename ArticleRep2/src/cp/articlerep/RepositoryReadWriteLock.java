package cp.articlerep;

import java.util.HashSet;

import cp.articlerep.ds.Iterator;
import cp.articlerep.ds.LinkedList;
import cp.articlerep.ds.List;
import cp.articlerep.ds.Map;
import cp.articlerep.ds.HashTable;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Ricardo Dias
 */
public class RepositoryReadWriteLock {

	private Map<String, List<Article>> byAuthor;
	private Map<String, List<Article>> byKeyword;
	private Map<Integer, Article> byArticleId;
	private final ReentrantReadWriteLock  lock =  new ReentrantReadWriteLock ();

	public RepositoryReadWriteLock(int nkeys) {
		this.byAuthor = new HashTable<String, List<Article>>(40000);
		this.byKeyword = new HashTable<String, List<Article>>(40000);
		this.byArticleId = new HashTable<Integer, Article>(40000);
	}

	public boolean insertArticle(Article a) {

		try{
			lock.writeLock().lock();
			if (byArticleId.contains(a.getId()))
				return false;
			byArticleId.put(a.getId(), a);

			Iterator<String> authors = a.getAuthors().iterator();
			while (authors.hasNext()) {
				String name = authors.next();

				List<Article> ll = byAuthor.get(name);
				if (ll == null) {
					ll = new LinkedList<Article>();
					byAuthor.put(name, ll);
				}
				ll.add(a);
			}

			Iterator<String> keywords = a.getKeywords().iterator();
			while (keywords.hasNext()) {
				String keyword = keywords.next();

				List<Article> ll = byKeyword.get(keyword);
				if (ll == null) {
					ll = new LinkedList<Article>();
					byKeyword.put(keyword, ll);
				} 
				ll.add(a);
				
			}
			//lock.writeLock().unlock();
		}catch( Exception e ) {
			  Thread.currentThread().interrupt();
			  e.printStackTrace();
			  return false;
		}
		
		finally{
			//if(lock.isWriteLockedByCurrentThread())
				lock.writeLock().unlock();
		}
		return true;
	}

	public void removeArticle(int id) {
		try{
			lock.writeLock().lock();
			Article a = byArticleId.get(id);
			if (a == null)
				return;
			Iterator<String> keywords = a.getKeywords().iterator();
			while (keywords.hasNext()) {
				String keyword = keywords.next();

				List<Article> ll = byKeyword.get(keyword);
				if (ll != null) {
					int pos = 0;
					Iterator<Article> it = ll.iterator();
					while (it.hasNext()) {
						Article toRem = it.next();
						if (toRem == a) {
							break;
						}
						pos++;
					}
					ll.remove(pos);
					it = ll.iterator();
					if (!it.hasNext()) { // checks if the list is empty
						byKeyword.remove(keyword);
					}
				}
			}

			Iterator<String> authors = a.getAuthors().iterator();
			while (authors.hasNext()) {
				String name = authors.next();

				List<Article> ll = byAuthor.get(name);
				if (ll != null) {
					int pos = 0;
					Iterator<Article> it = ll.iterator();
					while (it.hasNext()) {
						Article toRem = it.next();
						if (toRem == a) {
							break;
						}
						pos++;
					}
					ll.remove(pos);
					it = ll.iterator(); 
					if (!it.hasNext()) { // checks if the list is empty
						byAuthor.remove(name);
					}
				}
			}
			
			byArticleId.remove(id);
			//lock.writeLock().unlock();
		}catch( Exception e ) {
			  Thread.currentThread().interrupt();
			  e.printStackTrace();
		}
		
		finally{
			//if(lock.isWriteLockedByCurrentThread())
				lock.writeLock().unlock();
		}
	}

	public  List<Article> findArticleByAuthor(List<String> authors) {
		try{
			lock.readLock().lock();
			List<Article> res = new LinkedList<Article>();
			Iterator<String> it = authors.iterator();
			while (it.hasNext()) {
				String name = it.next();
				List<Article> as = byAuthor.get(name);
				if (as != null) {
					Iterator<Article> ait = as.iterator();
					while (ait.hasNext()) {
						Article a = ait.next();
						res.add(a);
					}
				}
			}

			return res;
		}catch( Exception e ) {
			  Thread.currentThread().interrupt();
			  e.printStackTrace();
			  return null;
		}
		finally{
			lock.readLock().unlock();
		}
		
	}

	public  List<Article> findArticleByKeyword(List<String> keywords) {
		try{
			lock.readLock().lock();
			List<Article> res = new LinkedList<Article>();

			Iterator<String> it = keywords.iterator();
			while (it.hasNext()) {
				String keyword = it.next();
				List<Article> as = byKeyword.get(keyword);
				if (as != null) {
					Iterator<Article> ait = as.iterator();
					while (ait.hasNext()) {
						Article a = ait.next();
						res.add(a);
					}
				}
			}
			return res;
		}catch( Exception e ) {
			  Thread.currentThread().interrupt();
			  e.printStackTrace();
			  return null;
		}
		
		finally{
			lock.readLock().unlock();
		}
		
	}

	
	/**
	 * This method is supposed to be executed with no concurrent thread
	 * accessing the repository.
	 * 
	 */
	public boolean validate() {
		//System.out.println("locks on wait:" + lock.getReadHoldCount());
		return checkById() && checkByAuthor() && checkByKeyword() ? true : false;
	}
	
	private boolean checkById(){
		HashSet<Integer> articleIds = new HashSet<Integer>();
		int articleCount = 0;
		//verificar se os autores e keywords existentes na hash existem nas respestivas hash's
		Iterator<Article> aIt = byArticleId.values();
		while(aIt.hasNext()) {
			Article a = aIt.next();
			
			//se houver repetidos, o count do hash não aumenta,
			// mas o articleCount aumenta!
			articleIds.add(a.getId());
			articleCount++;
			
			// check the authors consistency
			Iterator<String> authIt = a.getAuthors().iterator();
			while(authIt.hasNext()) {
				String name = authIt.next();
				if (!searchAuthorArticle(a, name)) {
					System.out.println(name + " is missing in by author");
					return false;
				}
			}
			
			// check the keywords consistency
			Iterator<String> keyIt = a.getKeywords().iterator();
			while(keyIt.hasNext()) {
				String keyword = keyIt.next();
				if (!searchKeywordArticle(a, keyword)) {
					System.out.println(keyword + " is missing in by keyword");
					return false;
				}
			}
		}
		
		return articleCount == articleIds.size();
	}
	
	private boolean searchAuthorArticle(Article a, String author) {
		List<Article> ll = byAuthor.get(author);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				if (it.next() == a) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean searchKeywordArticle(Article a, String keyword) {
		List<Article> ll = byKeyword.get(keyword);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				if (it.next() == a) {
					return true;
				}
			}
		}
		return false;
	}
	
	//verificar se os artigos contidos no byAuthor existem no byID
	private boolean checkByAuthor(){
			
			//verificar se os autores e keywords existentes na hash existem nas respestivas hash's
			Iterator<List<Article>> aIt = byAuthor.values();
			while(aIt.hasNext()) {
				List<Article> a = aIt.next();
				
				// check the authors consistency
				Iterator<Article> authIt = a.iterator();
				while(authIt.hasNext()) {
					
					Article article = authIt.next();
					int id = article.getId();
					
					if (!byArticleId.contains(id)) {
						System.out.println(id + " is missing in by id");
						return false;
					}
					
					List<String> keywords = article.getKeywords();
					Iterator<String> keyIt = keywords.iterator();
					
					while(keyIt.hasNext()) {
						String keyword = keyIt.next();
						if (!byKeyword.contains(keyword)) {
							System.out.println(keyword + " is missing in by keyword");
							return false;
						}
					}
				}
				
				
			}
			return true;
		}
		
	private boolean checkByKeyword(){
			
			//verificar se os autores e keywords existentes na hash existem nas respestivas hash's
			Iterator<List<Article>> kIt = byKeyword.values();
			while(kIt.hasNext()) {
				List<Article> k = kIt.next();
				
				// check the authors consistency
				Iterator<Article> artIt = k.iterator();
				while(artIt.hasNext()) {
					
					Article article = artIt.next();
					int id = article.getId();
					
					if (!byArticleId.contains(id)) {
						System.out.println(id + " is missing in by id");
						return false;
					}
					
					List<String> keywords = article.getAuthors();
					Iterator<String> authIt = keywords.iterator();
					
					while(authIt.hasNext()) {
						String author = authIt.next();
						if (!byAuthor.contains(author)) {
							System.out.println(author + " is missing in by author");
							return false;
						}
					}
				}
				
				
			}
			return true;
		}


}
