import java.util.Set;


/**
 *
 * @author arito
 * @param <T>
 */
public interface CRUD<T extends Object> {
    
    /**
     * Finds and returns all T objects
     * @return @{T}
     */
    Set<T> findAll();
    
    /**
     * 
     * @param <U>
     * @param objectId
     * @return T object
     */
    <U extends String> T findById(U objectId);
    
    /**
     * 
     * @param object
     * @return T object
     */
    T update(T object);
    
    /**
     * 
     * @param object
     * @return T object
     */
    T save(T object);
    
    /**
     * 
     * @param <U>
     * @param objectId 
     */
    <U extends String> void delete(U objectId);
    
}
