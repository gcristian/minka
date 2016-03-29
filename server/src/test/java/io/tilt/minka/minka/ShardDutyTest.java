/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.minka;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.tilt.minka.domain.ShardDuty;

public class ShardDutyTest {
    
    @Test
    public void tgst() {
        int i = (int) Math.ceil((double)20/3);
        System.out.println(i);
        Assert.assertTrue(true);
    }
    //@Test
    public void testBasic() {
        int i = (int) Math.ceil((double)20/3);
        System.out.println(i);
        Assert.assertTrue(true);

        
        Set<ShardDuty> set = new HashSet<>();
        DemoDuty d2 = new DemoDuty();
        set.add(ShardDuty.create(new DemoDuty()));
        set.add(ShardDuty.create(d2));
        set.add(ShardDuty.create(new DemoDuty()));
        assert(set.size() == 3);
        
        Assert.assertTrue(set.contains(ShardDuty.create(d2)));
    }
    
}
