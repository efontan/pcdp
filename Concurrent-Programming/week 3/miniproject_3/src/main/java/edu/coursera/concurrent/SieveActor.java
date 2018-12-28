package edu.coursera.concurrent;

import static edu.rice.pcdp.PCDP.finish;

import java.util.HashSet;
import java.util.Set;

import edu.rice.pcdp.Actor;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor
    extends Sieve {
    
    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        SieveActorActor sieveActor = new SieveActorActor();
        
        finish(() -> {
            for (int i = 2; i <= limit; i++) {
                sieveActor.send(i);
            }
            sieveActor.send(0);
        });
        
        int numPrimes = 0;
        SieveActorActor loopActor = sieveActor;
        while(loopActor != null) {
            numPrimes += loopActor.numLocalPrimes();
            loopActor = loopActor.nextActor;
        }
        
        return numPrimes;
    }
    
    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor
        extends Actor {
        
        private static final int MAX_LOCAL_PRIMES = 1000;
        
        private Set<Integer> localPrimes = new HashSet<>(MAX_LOCAL_PRIMES);
        private SieveActorActor nextActor;
        
        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            Integer candidate = (Integer) msg;
            
            // To exit from execution
            if (candidate <= 0) {
                if (this.nextActor != null) {
                    this.nextActor.send(msg);
                }
            } else {
                boolean locallyPrime = isLocalPrime(candidate);
                
                if (locallyPrime) {
                    if (this.localPrimes.size() < MAX_LOCAL_PRIMES) {
                        this.localPrimes.add(candidate);
                    } else {
                        if (nextActor == null) {
                            nextActor = new SieveActorActor();
                        }
    
                        nextActor.send(msg);
                    }
                }
            }
        }
        
        boolean isLocalPrime(int candidate) {
            for (Integer prime : localPrimes) {
                if (candidate % prime == 0) {
                    return false;
                }
            }
            return true;
        }
    
        public int numLocalPrimes() {
            return this.localPrimes.size();
        }
    }
}
